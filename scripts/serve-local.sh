#!/usr/bin/env bash
set -euo pipefail

cargo run -p tgdrive -- format --size "${TGDRIVE_SIZE:-64MiB}" --object-size "${TGDRIVE_OBJECT_SIZE_HUMAN:-256KiB}" --force
cargo run -p tgdrive -- serve-nbd --bind "${TGDRIVE_NBD_BIND:-127.0.0.1:10809}" --export-name "${TGDRIVE_NBD_EXPORT:-tgdrive}"
