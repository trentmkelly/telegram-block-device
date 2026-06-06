# Local NBD Setup

TGDrive exposes an NBD export over TCP. Linux can attach that export to a
`/dev/nbdX` device with `nbd-client`.

Early debugging should use a small device and ext2 before moving to ext4 or
LUKS. The current local backend is suitable for protocol testing; the Telegram
MTProto backend still needs to be wired before this behaves as durable remote
storage.

## Build And Format Local Metadata

```bash
cargo build --workspace
cargo run -p tgdrive -- format --size 64MiB --object-size 256KiB --force
```

## Start The NBD Export

```bash
cargo run -p tgdrive -- serve-nbd --bind 127.0.0.1:10809 --export-name tgdrive
```

## Attach The Kernel NBD Device

Run these from another shell:

```bash
sudo modprobe nbd max_part=8
sudo nbd-client 127.0.0.1 10809 /dev/nbd0 -N tgdrive
```

## Initial Filesystem Tests

```bash
sudo mkfs.ext2 /dev/nbd0
sudo mkdir -p /mnt/tgdrive
sudo mount -o noatime,nodiratime /dev/nbd0 /mnt/tgdrive
```

For ext4 after the basic path works:

```bash
sudo mkfs.ext4 -E lazy_itable_init=1,lazy_journal_init=1 /dev/nbd0
sudo mount -o noatime,nodiratime,commit=60 /dev/nbd0 /mnt/tgdrive
```

For encryption above TGDrive:

```bash
sudo cryptsetup luksFormat /dev/nbd0
sudo cryptsetup open /dev/nbd0 tgdrive_crypt
sudo mkfs.ext4 /dev/mapper/tgdrive_crypt
sudo mount -o noatime,nodiratime,commit=60 /dev/mapper/tgdrive_crypt /mnt/tgdrive
```

## Local Session And Cache Safety

TGDrive-native encryption is intentionally out of scope. Use LUKS or another
block-layer tool above `/dev/nbd0` for stored data. The Telegram session file,
SQLite metadata, and object cache are still local host artifacts, so protect the
machine with normal OS-level disk encryption if those files matter.

## Detach

```bash
sudo umount /mnt/tgdrive
sudo nbd-client -d /dev/nbd0
```
