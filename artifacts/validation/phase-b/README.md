# artifacts/validation/phase-b

Evidence for Phase B (ADR-074). **Read-only AWS only**: `sts`, `s3 ls`, Athena `SELECT`, and
an SSM command that reads Kafka end offsets. No apply, no EMR submission, no connector
change, no checkpoint touched. Account 111122223333, profile `my-aws-profile`, `ap-southeast-1`.

| file | what it shows |
|---|---|
| `00-identity.txt` | caller identity, profile, region, lake bucket |
| `01-checkpoints.txt` | `checkpoints/full_cdc/` holds only date-keyed `stream_w20260906`, `stream_w20260906b`; neither `full-cdc-oracle` nor `full-cdc-sqlserver` exists; no `/offsets/` under `warehouse/` |
| `02-state-before.txt` | `cdc-stream.py status`: both apps **ABSENT**. `health` exits **1**. The state table exists and nothing has written to it |
| `03-kafka-vs-lake.txt` | Kafka end offsets vs Iceberg row counts. `LOAN` 1,668 vs 65; `payment_method` 1,676 vs 74; `ACCOUNT` 737 vs 339 |
| `04-identity-duplicates.txt` | per table: `rows == distinct dv_event_id == distinct dv_src_event_id` (65 / 74 / 339); op mix; watermark 14:49 UTC at a 16:35 UTC reading |

**Reading `03`**: Kafka end offsets are summed across partitions and include tombstones (a
delete writes a `d` envelope *and* a tombstone), so they are an upper bound on events, not a
row count. The gap is still two orders of magnitude, and it is the ingest not running, not a
decode loss: the rows that did arrive are complete and duplicate-free (`04`).

**Not in this directory, and not claimed**: a resident run, a restart on one checkpoint, or
before/after offsets across that restart. Those are the pending live acceptance in
`docs/FULL_CDC_STREAMING.md` §7.
