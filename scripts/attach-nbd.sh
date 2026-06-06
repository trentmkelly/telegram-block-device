#!/usr/bin/env bash
set -euo pipefail

device="${TGDRIVE_NBD_DEVICE:-/dev/nbd0}"
host="${TGDRIVE_NBD_HOST:-127.0.0.1}"
port="${TGDRIVE_NBD_PORT:-10809}"
export_name="${TGDRIVE_NBD_EXPORT:-tgdrive}"

sudo modprobe nbd max_part=8
sudo nbd-client "$host" "$port" "$device" -N "$export_name"
