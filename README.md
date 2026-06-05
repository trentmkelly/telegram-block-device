# Telegram Block Device

This was vibe coded and had not been thoroughly reviewed for safety. Use it at
your own risk.

Telegram Block Device is a Linux block device backed by a private Telegram
channel. It speaks NBD locally, stores remote data as Telegram channel objects,
and treats Telegram as slow durable object storage rather than as a filesystem.

The daemon is intended to log in through Telegram MTProto, resolve a configured
storage channel, and recover its remote manifest from pinned channel metadata.
Local state is only a cache: a fresh machine should be able to rebuild its index
from the channel, hydrate blocks on demand, and regain access after a new
Telegram login.

Writes are staged locally, committed through copy-on-write object uploads, and
made durable remotely when the block device receives a flush. Reads use direct
message ID lookups and an aggressive local cache so ordinary Linux filesystems
can run on top of the exported device.

The storage layer deliberately avoids native encryption. It behaves like a
normal block device, so tools such as LUKS, ext4, and other standard Linux block
stack components can be layered above it.
