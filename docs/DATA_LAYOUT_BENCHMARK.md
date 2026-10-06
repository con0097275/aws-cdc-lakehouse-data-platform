# Data layout benchmark — 2026-09-20

**Conclusion: no production partition change is justified. The shipped layout wins or ties
every workload measured, and bucketing loses every one of them.**

Harness: `scripts/layout-benchmark.py` · raw data:
`artifacts/benchmark/layout-2026-09-20.json` · reproduce with
`python3 scripts/layout-benchmark.py --scale small --out <file>`.

## What was measured, and what was not

Every file count, file size, manifest count, snapshot count, partition count, scanned-file
and scanned-byte figure below is **observed** from a real Iceberg table built by real daily
commits on a seeded generator. Three gaps are stated rather than modelled:

| gap | why |
|---|---|
| **Athena bytes scanned is `null`** | The Glue catalog currently holds **0 tables**, so no Athena query can run. A benchmark that estimated the one metric everybody quotes would be worse than one that admits the hole. |
| **Absolute runtimes do not transfer** | `local[2]`, not a 4-executor EMR cluster. The **ratios between layouts on identical data** are what transfer. |
| **Bucket pruning for key lookups is NOT measured** | See §4. Its *costs* are measured; its claimed *benefit* is not. |

Scale: `small` — 1,000 / 20,000 / 100,000 rows over 30 days and 50 / 5,000 / 50,000 keys,
written as 30 daily commits per table (the shape a CDC ingest actually produces).

## 1. Layout statistics

| class | layout | partitions | files | avg file | min | max | manifests | snapshots | write amp |
|---|---|---|---|---|---|---|---|---|---|
| small | **identity(event_date)** | 30 | 30 | 3.2 KB | 3,117 | 3,404 | 30 | 30 | 1.00 |
| small | days(source_commit_ts) | 30 | 30 | 3.2 KB | 3,117 | 3,404 | 30 | 30 | 1.00 |
| small | +bucket(16) | 402 | 402 | 2.7 KB | 2,708 | 2,976 | 30 | 30 | 1.00 |
| small | +bucket(32) | 561 | 561 | 2.7 KB | 2,708 | 2,936 | 30 | 30 | 1.00 |
| small | +bucket(64) | 655 | 655 | 2.7 KB | 2,708 | 2,930 | 30 | 30 | 1.00 |
| small | unpartitioned | 1 | 60 | 3.0 KB | 2,909 | 3,242 | 30 | 30 | 1.00 |
| medium | **identity(event_date)** | 30 | 30 | 9.7 KB | 9,185 | 10,412 | 30 | 30 | 1.00 |
| medium | days(source_commit_ts) | 30 | 30 | 9.7 KB | 9,185 | 10,412 | 30 | 30 | 1.00 |
| medium | +bucket(16) | 480 | 480 | 3.3 KB | 3,180 | 3,735 | 30 | 30 | 1.00 |
| medium | +bucket(64) | 1,920 | 1,920 | 2.9 KB | 2,775 | 3,220 | 30 | 30 | 1.00 |
| medium | unpartitioned | 1 | 60 | 6.3 KB | 5,974 | 6,855 | 30 | 30 | 1.00 |
| hot | **identity(event_date)** | 30 | 30 | **38.9 KB** | 38,893 | 40,842 | 30 | 30 | 1.00 |
| hot | days(source_commit_ts) | 30 | 30 | **38.9 KB** | 38,893 | 40,842 | 30 | 30 | 1.00 |
| hot | +bucket(16) | 480 | 480 | 5.2 KB | 4,876 | 5,815 | 30 | 30 | 1.00 |
| hot | +bucket(32) | 960 | 960 | 4.1 KB | 3,773 | 4,619 | 30 | 30 | 1.00 |
| hot | +bucket(64) | 1,920 | 1,920 | 3.5 KB | 3,264 | 3,954 | 30 | 30 | 1.00 |
| hot | unpartitioned | 1 | 60 | 20.8 KB | 20,516 | 22,332 | 30 | 30 | 1.00 |

## 2. `days(dv_src_ldt)` vs the shipped `identity(event_date)` — **identical**

Across all three classes the two layouts produce the **same partition count, the same file
count, and byte-identical min/avg/max file sizes**, and every workload reads the same files
and bytes. ADR-062 argued they are the same pruning with the transform applied at write time;
this measures it.

The shipped layout is therefore kept, and the reason is now evidence rather than assertion.
It also has one property the measurement made visible: `event_date` is materialised **once,
at write time, in a pinned session zone**, while `days(source_commit_ts)` re-derives the day
from a timestamp in whatever zone the reader has. An early run of this harness produced
60 partitions for `days()` against 30 for `identity()` purely because naive datetimes were
reinterpreted in the machine's zone — exactly the conversion that corrupted watermarks in
Phase D. That fragility is a real difference between the two, in favour of materialising.

## 3. Workloads — files read / bytes read (identity vs bucket vs unpartitioned)

**hot table**, the class where a layout change would matter most:

| workload | identity | +bucket(16) | +bucket(64) | unpartitioned |
|---|---|---|---|---|
| FULL_CDC + 1 day | **1 file / 39 KB** | 16 / 83 KB | 64 / 224 KB | 60 / 1,246 KB |
| FULL_CDC + 7 days | **21 / 816 KB** | 336 / 1,743 KB | 1,344 / 4,690 KB | 60 / 1,246 KB |
| REALTIME build | **21 / 816 KB** · 61 ms | 336 / 1,743 KB · 236 ms | 1,344 / 4,690 KB · **761 ms** | 60 / 1,246 KB · 92 ms |
| EOD build | **30 / 1,166 KB** · 119 ms | 480 / 2,491 KB · 289 ms | 1,920 / 6,701 KB · **775 ms** | 60 / 1,246 KB · 136 ms |
| AUTO_CORRECT | **21 / 816 KB** · 70 ms | 336 / 1,743 KB · 238 ms | 1,344 / 4,690 KB · **776 ms** | 60 / 1,246 KB · 90 ms |
| FULFILL | **1 / 39 KB** | 16 / 83 KB | 64 / 224 KB | 60 / 1,246 KB |

Reading across: a one-day FULL_CDC scan touches **1 file and 39 KB** under the shipped layout,
**5.7×** the bytes under `bucket(64)`, and **32×** the bytes unpartitioned. Bucketing makes
every date-predicated workload worse at every scale, and the penalty grows with N.

The mechanism is visible in §1: bucketing multiplies partitions by N while the data per day
stays the same, so a day's single 38.9 KB file becomes 64 files of 3.5 KB. That is the
small-file problem the maintenance framework exists to fix, created deliberately at write
time.

## 4. Bucketing: costs measured, benefit **not** measured

Bucketing exists to prune a **key-equality** lookup, which a date partition cannot. This
harness does **not** measure that pruning: for the `key_lookup` workload it reports the whole
table's files for every layout, because the metadata probe has no bucket-aware predicate. The
runtime column shows no advantage (hot: 50 ms identity vs 41–45 ms bucketed — inside noise on
`local[2]`).

So the honest position is asymmetric and stated as such: **bucketing's costs are measured and
large; its benefit is unmeasured.** That is enough to keep it off — the shipped default,
`bucket(N)` supported in the schema and enabled for no table — and not enough to call it
useless. What would justify revisiting:

1. a workload that is genuinely key-equality dominated (a PIT lookup service, not a mart);
2. a bucket-aware scan measurement (`.files` filtered on the bucket partition field, or an
   EMR run with Spark scan metrics);
3. a table large enough that a day's partition exceeds the 128 MiB target, so splitting it
   costs nothing in file size. **No table in this registry is near that**: the hot class at
   100k rows/30 days produces 38.9 KB files, three orders of magnitude below target.

## 5. Write ordering and distribution

All tables were written with `write.distribution-mode=hash` and zstd, matching production.
Write amplification is **1.00 everywhere** — no layout caused rewrites during the load, so
the file-count differences above are purely a partitioning effect, not an artefact of
different write paths. Manifest and snapshot counts are identical (30/30) across all six
layouts, so neither metric distinguishes them at this scale.

## 6. Recommendation per table class

| class | partition | bucket | reason |
|---|---|---|---|
| small (reference: `channel`, `BRANCH`) | `identity(event_date)` | **no** | 30 files of 3.2 KB already; bucketing makes 402–655 files of 2.7 KB for a table that fits in one. |
| medium (`account`, `payment_method`) | `identity(event_date)` | **no** | 5.7× more bytes read on a one-day scan under bucket(64), no measured benefit. |
| hot (`digital_event`, `TRANSACTION`) | `identity(event_date)` | **no** | The worst case for bucketing: 1,920 files at N=64, 775 ms EOD build against 119 ms. |

**No global bucket policy, and no per-class bucket policy either — because no class showed a
benefit.** The mechanism stays in the schema (ADR-062) for the workload that would justify it.

## 7. Config changes made

**None.** The brief's instruction was not to change production partitioning without measured
evidence; the evidence says the current configuration is already the best of those tested.
`cdc/registry/sources.yaml` keeps `partition_spec: [{column: event_date, transform: identity}]`
and bucketing off for every table.

## 8. Pending

* Athena bytes scanned, per workload per layout — needs a populated Glue catalog.
* A bucket-aware pruning measurement (§4).
* An EMR-scale run: these are `local[2]` numbers, and the file-size ratios matter more than
  the milliseconds.
