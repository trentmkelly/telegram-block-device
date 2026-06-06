use std::{net::SocketAddr, sync::Arc, time::Instant};

use async_trait::async_trait;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    sync::Semaphore,
};
use tracing::{debug, info, warn};

const NBD_MAGIC: u64 = 0x4e42_444d_4147_4943;
const IHAVEOPT: u64 = 0x4948_4156_454f_5054;
const OPTION_REPLY_MAGIC: u64 = 0x0003_e889_0455_65a9;
const REQ_MAGIC: u32 = 0x2560_9513;
const REP_MAGIC: u32 = 0x6744_6698;

const HANDSHAKE_FIXED_NEWSTYLE: u16 = 1;
const HANDSHAKE_NO_ZEROES: u16 = 2;

const TRANSMISSION_HAS_FLAGS: u16 = 1;
const TRANSMISSION_SEND_FLUSH: u16 = 4;
const TRANSMISSION_SEND_TRIM: u16 = 32;
const TRANSMISSION_SEND_WRITE_ZEROES: u16 = 64;

const OPT_EXPORT_NAME: u32 = 1;
const OPT_ABORT: u32 = 2;
const OPT_GO: u32 = 7;
const OPT_STRUCTURED_REPLY: u32 = 8;

const OPT_REP_ACK: u32 = 1;
const OPT_REP_INFO: u32 = 3;
const OPT_REP_ERR_UNSUP: u32 = 0x8000_0001;
const OPT_REP_ERR_INVALID: u32 = 0x8000_0003;

const INFO_EXPORT: u16 = 0;

const CMD_READ: u16 = 0;
const CMD_WRITE: u16 = 1;
const CMD_DISC: u16 = 2;
const CMD_FLUSH: u16 = 3;
const CMD_TRIM: u16 = 4;
const CMD_WRITE_ZEROES: u16 = 6;

const ERR_UNSUPPORTED: u32 = 95;
const ERR_INVALID: u32 = 22;

#[derive(Debug, Clone)]
pub struct NbdServerOptions {
    pub bind: String,
    pub export_name: String,
    pub device_size: u64,
    pub logical_block_size: u32,
    pub max_inflight: usize,
}

impl Default for NbdServerOptions {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:10809".to_string(),
            export_name: "tgdrive".to_string(),
            device_size: 64 * 1024 * 1024,
            logical_block_size: 4096,
            max_inflight: 64,
        }
    }
}

#[async_trait]
pub trait NbdBackend: Send + Sync + 'static {
    async fn read_at(&self, offset: u64, len: usize) -> anyhow::Result<Vec<u8>>;
    async fn write_at(&self, offset: u64, bytes: &[u8]) -> anyhow::Result<()>;
    async fn flush(&self) -> anyhow::Result<()>;
}

pub struct NbdExport<B> {
    pub options: NbdServerOptions,
    backend: Arc<B>,
}

impl<B: NbdBackend> NbdExport<B> {
    pub fn new(options: NbdServerOptions, backend: Arc<B>) -> Self {
        Self { options, backend }
    }

    pub async fn serve(self) -> anyhow::Result<()> {
        let listener = TcpListener::bind(&self.options.bind).await?;
        info!(bind = %self.options.bind, "NBD server listening");
        loop {
            let (stream, peer) = listener.accept().await?;
            let backend = Arc::clone(&self.backend);
            let options = self.options.clone();
            tokio::spawn(async move {
                if let Err(err) = handle_client(stream, peer, options, backend).await {
                    warn!(%peer, error = %err, "NBD client disconnected with error");
                }
            });
        }
    }
}

#[async_trait]
impl<B: tgdrive_core::RemoteBackend + 'static> NbdBackend for tgdrive_core::BlockDevice<B> {
    async fn read_at(&self, offset: u64, len: usize) -> anyhow::Result<Vec<u8>> {
        self.read_at(offset, len).await
    }

    async fn write_at(&self, offset: u64, bytes: &[u8]) -> anyhow::Result<()> {
        self.write_at(offset, bytes).await
    }

    async fn flush(&self) -> anyhow::Result<()> {
        self.flush().await
    }
}

async fn handle_client<S, B>(
    mut stream: S,
    peer: SocketAddr,
    options: NbdServerOptions,
    backend: Arc<B>,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    B: NbdBackend,
{
    negotiate(&mut stream, &options).await?;
    let semaphore = Arc::new(Semaphore::new(options.max_inflight.max(1)));
    loop {
        let request = match Request::read_from(&mut stream).await {
            Ok(request) => request,
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(err) => return Err(err.into()),
        };
        let _permit = semaphore.acquire().await?;
        let started = Instant::now();
        debug!(%peer, command = request.command, offset = request.offset, len = request.length, "NBD request");
        match request.command {
            CMD_READ => {
                if !bounds_ok(options.device_size, request.offset, request.length) {
                    write_reply(&mut stream, ERR_INVALID, request.handle).await?;
                    continue;
                }
                let data = backend
                    .read_at(request.offset, request.length as usize)
                    .await?;
                write_reply(&mut stream, 0, request.handle).await?;
                stream.write_all(&data).await?;
            }
            CMD_WRITE => {
                if !bounds_ok(options.device_size, request.offset, request.length) {
                    discard_payload(&mut stream, request.length).await?;
                    write_reply(&mut stream, ERR_INVALID, request.handle).await?;
                    continue;
                }
                let mut payload = vec![0u8; request.length as usize];
                stream.read_exact(&mut payload).await?;
                backend.write_at(request.offset, &payload).await?;
                write_reply(&mut stream, 0, request.handle).await?;
            }
            CMD_FLUSH => {
                backend.flush().await?;
                write_reply(&mut stream, 0, request.handle).await?;
            }
            CMD_DISC => {
                backend.flush().await?;
                return Ok(());
            }
            CMD_TRIM | CMD_WRITE_ZEROES => {
                if request.length > 0 && request.command == CMD_WRITE {
                    discard_payload(&mut stream, request.length).await?;
                }
                write_reply(&mut stream, ERR_UNSUPPORTED, request.handle).await?;
            }
            _ => {
                if request.command == CMD_WRITE {
                    discard_payload(&mut stream, request.length).await?;
                }
                write_reply(&mut stream, ERR_UNSUPPORTED, request.handle).await?;
            }
        }
        stream.flush().await?;
        debug!(
            %peer,
            command = request.command,
            elapsed_ms = started.elapsed().as_millis(),
            "NBD request completed"
        );
    }
}

async fn negotiate<S>(stream: &mut S, options: &NbdServerOptions) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    stream.write_u64(NBD_MAGIC).await?;
    stream.write_u64(IHAVEOPT).await?;
    stream
        .write_u16(HANDSHAKE_FIXED_NEWSTYLE | HANDSHAKE_NO_ZEROES)
        .await?;
    stream.flush().await?;

    let _client_flags = stream.read_u32().await?;
    loop {
        let magic = stream.read_u64().await?;
        anyhow::ensure!(magic == IHAVEOPT, "invalid option magic");
        let option = stream.read_u32().await?;
        let length = stream.read_u32().await?;
        let mut data = vec![0u8; length as usize];
        stream.read_exact(&mut data).await?;
        match option {
            OPT_EXPORT_NAME => {
                let requested = String::from_utf8_lossy(&data);
                anyhow::ensure!(
                    requested.is_empty() || requested == options.export_name,
                    "unknown NBD export {requested}"
                );
                write_export_info(stream, options).await?;
                stream.write_all(&[0u8; 124]).await?;
                stream.flush().await?;
                return Ok(());
            }
            OPT_GO => {
                let Some(requested) = parse_go_export_name(&data) else {
                    write_option_reply(stream, option, OPT_REP_ERR_INVALID, b"invalid NBD_OPT_GO")
                        .await?;
                    stream.flush().await?;
                    continue;
                };
                if !requested.is_empty() && requested != options.export_name {
                    write_option_reply(stream, option, OPT_REP_ERR_INVALID, b"unknown NBD export")
                        .await?;
                    stream.flush().await?;
                    continue;
                }
                let mut info = Vec::with_capacity(12);
                info.extend_from_slice(&INFO_EXPORT.to_be_bytes());
                info.extend_from_slice(&options.device_size.to_be_bytes());
                info.extend_from_slice(&transmission_flags().to_be_bytes());
                write_option_reply(stream, option, OPT_REP_INFO, &info).await?;
                write_option_reply(stream, option, OPT_REP_ACK, &[]).await?;
                stream.flush().await?;
                return Ok(());
            }
            OPT_STRUCTURED_REPLY => {
                write_option_reply(
                    stream,
                    option,
                    OPT_REP_ERR_UNSUP,
                    b"structured replies are not supported",
                )
                .await?;
                stream.flush().await?;
            }
            OPT_ABORT => anyhow::bail!("client aborted negotiation"),
            _ => {
                write_option_reply(stream, option, OPT_REP_ERR_UNSUP, b"unsupported NBD option")
                    .await?;
                stream.flush().await?;
            }
        }
    }
}

async fn write_export_info<S>(stream: &mut S, options: &NbdServerOptions) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    stream.write_u64(options.device_size).await?;
    stream.write_u16(transmission_flags()).await?;
    Ok(())
}

fn transmission_flags() -> u16 {
    TRANSMISSION_HAS_FLAGS
        | TRANSMISSION_SEND_FLUSH
        | TRANSMISSION_SEND_TRIM
        | TRANSMISSION_SEND_WRITE_ZEROES
}

async fn write_option_reply<S>(
    stream: &mut S,
    option: u32,
    reply: u32,
    payload: &[u8],
) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    stream.write_u64(OPTION_REPLY_MAGIC).await?;
    stream.write_u32(option).await?;
    stream.write_u32(reply).await?;
    stream.write_u32(payload.len() as u32).await?;
    stream.write_all(payload).await?;
    Ok(())
}

fn parse_go_export_name(data: &[u8]) -> Option<String> {
    if data.len() < 6 {
        return None;
    }
    let name_len = u32::from_be_bytes(data[0..4].try_into().ok()?) as usize;
    if data.len() < 4 + name_len + 2 {
        return None;
    }
    let name = String::from_utf8_lossy(&data[4..4 + name_len]).to_string();
    Some(name)
}

#[derive(Debug)]
struct Request {
    command: u16,
    handle: u64,
    offset: u64,
    length: u32,
}

impl Request {
    async fn read_from<S>(stream: &mut S) -> std::io::Result<Self>
    where
        S: AsyncRead + Unpin,
    {
        let magic = stream.read_u32().await?;
        if magic != REQ_MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid request magic",
            ));
        }
        let _flags = stream.read_u16().await?;
        let command = stream.read_u16().await?;
        let handle = stream.read_u64().await?;
        let offset = stream.read_u64().await?;
        let length = stream.read_u32().await?;
        Ok(Self {
            command,
            handle,
            offset,
            length,
        })
    }
}

async fn write_reply<S>(stream: &mut S, error: u32, handle: u64) -> anyhow::Result<()>
where
    S: AsyncWrite + Unpin,
{
    stream.write_u32(REP_MAGIC).await?;
    stream.write_u32(error).await?;
    stream.write_u64(handle).await?;
    Ok(())
}

async fn discard_payload<S>(stream: &mut S, len: u32) -> anyhow::Result<()>
where
    S: AsyncRead + Unpin,
{
    let mut remaining = len as usize;
    let mut buf = [0u8; 8192];
    while remaining > 0 {
        let take = remaining.min(buf.len());
        stream.read_exact(&mut buf[..take]).await?;
        remaining -= take;
    }
    Ok(())
}

fn bounds_ok(device_size: u64, offset: u64, len: u32) -> bool {
    offset
        .checked_add(u64::from(len))
        .map(|end| end <= device_size)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};

    use super::*;

    struct MemNbd {
        bytes: Mutex<Vec<u8>>,
        flushes: Mutex<usize>,
    }

    #[async_trait]
    impl NbdBackend for MemNbd {
        async fn read_at(&self, offset: u64, len: usize) -> anyhow::Result<Vec<u8>> {
            let bytes = self.bytes.lock().unwrap();
            Ok(bytes[offset as usize..offset as usize + len].to_vec())
        }

        async fn write_at(&self, offset: u64, payload: &[u8]) -> anyhow::Result<()> {
            let mut bytes = self.bytes.lock().unwrap();
            bytes[offset as usize..offset as usize + payload.len()].copy_from_slice(payload);
            Ok(())
        }

        async fn flush(&self) -> anyhow::Result<()> {
            *self.flushes.lock().unwrap() += 1;
            Ok(())
        }
    }

    #[tokio::test]
    async fn negotiates_and_serves_read_write_flush_disconnect() {
        let (mut client, server) = duplex(64 * 1024);
        let options = NbdServerOptions {
            bind: "127.0.0.1:0".to_string(),
            export_name: "tgdrive".to_string(),
            device_size: 4096,
            logical_block_size: 4096,
            max_inflight: 8,
        };
        let backend = Arc::new(MemNbd {
            bytes: Mutex::new(vec![0; 4096]),
            flushes: Mutex::new(0),
        });
        let server_backend = Arc::clone(&backend);
        tokio::spawn(async move {
            handle_client(
                server,
                "127.0.0.1:1".parse().unwrap(),
                options,
                server_backend,
            )
            .await
            .unwrap();
        });

        assert_eq!(client.read_u64().await.unwrap(), NBD_MAGIC);
        assert_eq!(client.read_u64().await.unwrap(), IHAVEOPT);
        assert_eq!(
            client.read_u16().await.unwrap(),
            HANDSHAKE_FIXED_NEWSTYLE | HANDSHAKE_NO_ZEROES
        );
        client.write_u32(0).await.unwrap();
        client.write_u64(IHAVEOPT).await.unwrap();
        client.write_u32(OPT_EXPORT_NAME).await.unwrap();
        client.write_u32(7).await.unwrap();
        client.write_all(b"tgdrive").await.unwrap();
        assert_eq!(client.read_u64().await.unwrap(), 4096);
        let flags = client.read_u16().await.unwrap();
        assert!(flags & TRANSMISSION_SEND_FLUSH != 0);
        let mut zeros = [0u8; 124];
        client.read_exact(&mut zeros).await.unwrap();

        write_request(&mut client, CMD_WRITE, 1, 4, b"test").await;
        assert_reply(&mut client, 0, 1).await;
        write_request_header(&mut client, CMD_READ, 2, 4, 4).await;
        assert_reply(&mut client, 0, 2).await;
        let mut data = [0u8; 4];
        client.read_exact(&mut data).await.unwrap();
        assert_eq!(&data, b"test");
        write_request_header(&mut client, CMD_FLUSH, 3, 0, 0).await;
        assert_reply(&mut client, 0, 3).await;
        write_request_header(&mut client, CMD_DISC, 4, 0, 0).await;
    }

    #[tokio::test]
    async fn rejects_trim_and_write_zeroes() {
        let (mut client, server) = duplex(64 * 1024);
        let options = NbdServerOptions {
            device_size: 4096,
            ..NbdServerOptions::default()
        };
        let backend = Arc::new(MemNbd {
            bytes: Mutex::new(vec![0; 4096]),
            flushes: Mutex::new(0),
        });
        tokio::spawn(async move {
            handle_client(server, "127.0.0.1:1".parse().unwrap(), options, backend)
                .await
                .unwrap();
        });
        finish_handshake(&mut client).await;
        write_request_header(&mut client, CMD_TRIM, 1, 0, 512).await;
        assert_reply(&mut client, ERR_UNSUPPORTED, 1).await;
        write_request_header(&mut client, CMD_WRITE_ZEROES, 2, 0, 512).await;
        assert_reply(&mut client, ERR_UNSUPPORTED, 2).await;
    }

    async fn finish_handshake(client: &mut (impl AsyncReadExt + AsyncWriteExt + Unpin)) {
        let _ = client.read_u64().await.unwrap();
        let _ = client.read_u64().await.unwrap();
        let _ = client.read_u16().await.unwrap();
        client.write_u32(0).await.unwrap();
        client.write_u64(IHAVEOPT).await.unwrap();
        client.write_u32(OPT_EXPORT_NAME).await.unwrap();
        client.write_u32(7).await.unwrap();
        client.write_all(b"tgdrive").await.unwrap();
        let _ = client.read_u64().await.unwrap();
        let _ = client.read_u16().await.unwrap();
        let mut zeros = [0u8; 124];
        client.read_exact(&mut zeros).await.unwrap();
    }

    async fn write_request(
        client: &mut (impl AsyncWriteExt + Unpin),
        command: u16,
        handle: u64,
        offset: u64,
        payload: &[u8],
    ) {
        write_request_header(client, command, handle, offset, payload.len() as u32).await;
        client.write_all(payload).await.unwrap();
    }

    async fn write_request_header(
        client: &mut (impl AsyncWriteExt + Unpin),
        command: u16,
        handle: u64,
        offset: u64,
        len: u32,
    ) {
        client.write_u32(REQ_MAGIC).await.unwrap();
        client.write_u16(0).await.unwrap();
        client.write_u16(command).await.unwrap();
        client.write_u64(handle).await.unwrap();
        client.write_u64(offset).await.unwrap();
        client.write_u32(len).await.unwrap();
    }

    async fn assert_reply(client: &mut (impl AsyncReadExt + Unpin), error: u32, handle: u64) {
        assert_eq!(client.read_u32().await.unwrap(), REP_MAGIC);
        assert_eq!(client.read_u32().await.unwrap(), error);
        assert_eq!(client.read_u64().await.unwrap(), handle);
    }
}
