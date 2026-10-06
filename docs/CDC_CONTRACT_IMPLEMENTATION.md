# CDC Contract Implementation — Debezium → Avro

- Session: 05
- Date: 2026-08-13
- Status: **`static-validated` only. CDC CORRECTNESS HAS NOT BEEN DEMONSTRATED.**
- Contract: [`docs/DATA_CONTRACTS.md`](DATA_CONTRACTS.md) §3–5 and §10

> ## ⛔ Read this before trusting anything below
>
> **No connector has ever run.** The account holds **0 MSK clusters and 0 EC2 instances**
> (verified 2026-08-13), and the Terraform state backend does not exist. Every statement
> in this document describes what the configuration *specifies*, not what has been
> *observed*.
>
> Section 6's test matrix lists every acceptance criterion as **`NOT_TESTED`**, with the
> exact command that would test it. That is not an oversight — it is the honest status.

---

## 1. Source-position mapping — the heart of ordering

`docs/DATA_CONTRACTS.md` §4 defines `event_order` as a **struct**, not a scalar, because
ordering needs up to five components and flattening them is how subtle ordering bugs get
introduced. This is where each component comes from.

| `event_order` field | Oracle (LogMiner) | SQL Server (CDC) |
|---|---|---|
| `position_primary` | `source.commit_scn` | `source.commit_lsn` |
| `position_secondary` | `source.scn` (change SCN) | `source.change_lsn` (see note) |
| `source_ts_ms` | `source.ts_ms` | `source.ts_ms` |
| `kafka_partition` | Kafka metadata | Kafka metadata |
| `kafka_offset` | Kafka metadata | Kafka metadata |

### 1.1 Why the comparison cannot be a naive string compare

`DATA_CONTRACTS.md` §4.2 is normative and easy to get wrong in opposite directions:

- **Oracle SCN is numeric.** Lexicographically `'9' > '10'`, but numerically `9 < 10`.
  Comparing SCNs as strings sorts them wrongly the moment they cross a digit boundary.
- **SQL Server LSN is a zero-padded hex triplet** (`0000002A:00000C38:0001`).
  Lexicographic compare happens to be correct **only because** every component is
  fixed-width. Strip the padding and it breaks.

Normative rule: both positions are stored **already normalised to fixed-width,
zero-padded, lexicographically sortable form**. The normalisation belongs to the L1
writer (Session 06), and this document records what it must consume.

### 1.2 The single configuration choice that makes this possible

Both connectors set **`"transforms": ""`** — no `ExtractNewRecordState`, deliberately.

Unwrapping is the most common Debezium tutorial step and it would **silently destroy this
contract**. It strips the envelope down to the `after` image, deleting `op`, `before`,
`ts_ms` and the entire `source` block — including every SCN and LSN field the table above
depends on. `CLAUDE.md` §5.2 requires L1 to preserve all source and Kafka metadata; a
transform here would make correct ordering impossible downstream, and nothing would fail
loudly at the time.

`scripts/cdc-correctness-test.sh` test 3 asserts the envelope fields are present, so a
transform added later is caught rather than assumed absent.

## 2. Key and partitioning

`CLAUDE.md` §5.1: the topic key is the canonical PK, so the same PK always lands in the
same partition. `message.key.columns` states this explicitly per table rather than
relying on Debezium's PK default — a source PK change would otherwise silently
repartition a topic.

| Table | Key |
|---|---|
| `COREBANK.CUSTOMER` / `ACCOUNT` / `TRANSACTION` / `BRANCH` | `CUSTOMER_ID` / `ACCOUNT_ID` / `TRANSACTION_ID` / `BRANCH_ID` |
| `dbo.app_user` / `digital_event` / `channel` / `merchant` | `user_id` / `event_id` / `channel_code` / `merchant_id` |

Topic partition counts are **fixed at 3 and permanent**. Changing them re-hashes keys and
breaks the guarantee for every record already written — test 2 verifies no key spans more
than one partition by reading actual `(key, partition)` pairs, not by reading config.

## 3. Type handling

| Setting | Value | Why |
|---|---|---|
| `decimal.handling.mode` | **`precise`** | Oracle `NUMBER(18,2)` must arrive as a Connect `Decimal`. `double` makes every downstream sum non-reproducible, and the error is invisible until a reconciliation fails by cents |
| `time.precision.mode` | `adaptive_time_microseconds` (Oracle) / `adaptive` (SQL Server) | Preserves source precision rather than truncating to milliseconds |
| `tombstones.on.delete` | **`true`** | `CLAUDE.md` §5.7 — a delete emits a `d` envelope **and** a null-valued tombstone. Both must reach L1, or deletes become ambiguous |

## 4. Snapshot mode — and the one that would break reconciliation

Both connectors use **`snapshot.mode: initial`**: snapshot, then stream.

**Not `schema_only`.** That skips existing rows, so the 2 000 seeded Oracle transactions
and 3 000 SQL Server events would never reach L1 — and reconciliation would fail by
*exactly the seed size*, which looks like a subtle bug rather than a configuration choice.

**Not `initial_only`.** That stops after the snapshot and never streams.

## 5. Secrets

No connector config contains a password. `database.password` is
`${ssm:/kafka-dev-lab/dev/source-lab/<engine>-cdc-password}`, resolved by the SSM config
provider at task start (ADR-031, `CLAUDE.md` §3.1). The literal never exists in the
connector config, in the `connect-configs` topic, or in a REST API response.

Connectors authenticate as `c##dbzuser` / `dbzuser` — **never `SYS` or `sa`** (S03-8).

## 6. Test matrix — every criterion, current status

Acceptance criteria from `sessions/05_debezium_oracle_sqlserver_avro.md`.

| # | Criterion | Test | Status |
|---|---|---|---|
| 1 | I/U/D from both sources appear as Avro on the right topics | `cdc-correctness-test.sh 1` | **`NOT_TESTED`** |
| 2 | Registry subjects and versions are clear | ibid. 5 | **`NOT_TESTED`** |
| 3 | Same-PK ordering holds | ibid. 2 | **`NOT_TESTED`** |
| 4 | Connector restart does not reset the snapshot | ibid. 4 | **`NOT_TESTED`** |
| 5 | Incompatible schema is blocked or quarantined | ibid. 6 | **`NOT_TESTED`** |
| 6 | Source position (SCN/LSN) present and monotonic | ibid. 3 | **`NOT_TESTED`** |
| 7 | No plaintext password in connector config | static | **`static-validated` — PASS** |
| 8 | No unwrap transform; envelope preserved | static | **`static-validated` — PASS** |
| 9 | `decimal.handling.mode = precise` | static | **`static-validated` — PASS** |
| 10 | Tombstones enabled | static | **`static-validated` — PASS** |

**Four of ten are statically verifiable and pass. The six that matter most — the ones
that prove CDC actually works — cannot be run.** They require a live MSK cluster, live
source databases and a live Connect worker, none of which exist.

### What would demonstrate correctness

In order. Each gates the next:

```bash
# 0. the stack must exist at all
bash scripts/bootstrap-state-backend.sh --execute       # needs a human-typed phrase
terraform -chdir=terraform/envs/dev init -backend-config=backend.hcl
bash scripts/tf.sh apply --execute

# 1. sources must actually be capturing (risk R15)
bash scripts/source-lab.sh enable-cdc --execute
bash scripts/source-lab.sh seed --execute
bash scripts/source-lab.sh verify-cdc                   # MUST pass

# 2. the runtime must actually reach MSK (risks R2, R3)
bash scripts/cdc-runtime.sh smoke                       # MUST pass, incl. schema WRITE

# 3. only now do connectors mean anything
bash scripts/register-connectors.sh register --execute
bash scripts/cdc-runtime.sh correctness all
```

Steps 1 and 2 are not ceremony. **A Debezium connector against a CDC-disabled table
starts `RUNNING` and produces nothing**, and **Apicurio returns 200 on health while
500-ing every write**. Running step 3 first produces a green dashboard and no data.

## 7. Rollback

```bash
bash scripts/register-connectors.sh delete --execute    # offsets RETAINED — re-register resumes
bash scripts/register-connectors.sh reset  --execute    # DESTRUCTIVE: forces a full re-snapshot
```

`delete` is safe: offsets live in `connect-offsets`, so re-registering resumes where the
connector stopped (ADR-002). `reset` deletes those offsets, so the next registration
re-emits **every** row — duplicates that only `event_id` idempotency absorbs
(`CLAUDE.md` §5.3).

> **Note on `event_serial_no`.** An earlier version of this table listed it as a further
> component of `position_secondary`. The live canonical path
> (`spark/jobs/full_cdc/job.py`) writes `source.change_lsn` and nothing else, and that is
> correct: SQL Server gives every ROW CHANGE its own `change_lsn` within a transaction while
> `commit_lsn` is shared by the whole transaction, so `(commit_lsn, change_lsn)` already
> separates every row change. `event_serial_no` distinguishes only the constituent parts of
> one change -- the delete/insert pair behind an update -- and the connector emits those as a
> single `u` envelope, so the platform never sees two events sharing both LSNs.
>
> `spark/jobs/l1_stream/ordering.py` still normalises `event_serial_no`. That module belongs
> to the SUPERSEDED L1 path (its own docstring records that its Avro decode was never
> written) and is not what runs. Pinned by
> `spark/tests/test_ordering.py::TestLivePathSecondaryIsChangeLsn`.
