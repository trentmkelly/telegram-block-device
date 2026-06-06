# Performance Notes

These are early local-backend measurements, useful for implementation tuning but
not a substitute for live Telegram and mounted filesystem benchmarks.

Command:

```bash
cargo run -p tgdrive -- bench --size 16MiB --object-size 256KiB --ops 16
cargo run -p tgdrive -- bench --size 16MiB --object-size 1MiB --ops 16
```

Results:

| object size | sequential write | first flush | sequential read | random read | random write | final flush | uploads |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 KiB | 11357 ms | 669 ms | 27565 ms | 1272 ms | 5130 ms | 307 ms | 80 |
| 1 MiB | 4934 ms | 617 ms | 6007 ms | 1601 ms | 7020 ms | 682 ms | 30 |

Initial default remains `256 KiB` because it limits read-modify-write
amplification for filesystem metadata and small random writes. The `1 MiB`
profile is promising for sequential workloads and should be retested after the
live Telegram path and mounted ext2/ext4 workloads are available.
