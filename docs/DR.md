# Disaster recovery — RPO, RTO and the limits of a lab

- Session: 15
- Date: 2026-08-15
- Status: **procedures written; 4 recovery properties demonstrated locally, the rest `NOT_TESTED`**

Extends the RPO/RTO table in `docs/TARGET_ARCHITECTURE.md` §"Failure / Detection /
Recovery". It does not contradict it.

---

## 1. The one number that governs everything

**Kafka retention is 24 hours.**

Every RPO in this document is a consequence of that. Inside the window, recovery is a replay
and RPO is 0 — the events are still in Kafka and every write is idempotent. Outside it, the
events are gone and the only recovery is a Debezium re-snapshot, which is RPO 24h.

That single fact is why `KafkaConsumerLagGrowing` pages while `NrtFreshnessSloBreached` only
tickets: lag that keeps growing converts a latency problem into a data-loss problem, and it
does so on a clock.

## 2. RPO / RTO by scenario

| # | Scenario | RPO | RTO | Recovery | Demonstrated |
|---|---|---|---|---|---|
| 1 | Connect worker dies | **0** | ~2 min | restart; offsets in internal topics | `NOT_TESTED` |
| 2 | Debezium connector restart | **0** | ~3 min | resume from stored offset, no re-snapshot | `NOT_TESTED` |
| 3 | Spark streaming job dies | **0** | ~5 min | restart from checkpoint | `NOT_TESTED` |
| 4 | Checkpoint lost | **0** *within retention* | ~15 min | restart from earliest offset; idempotent on `event_id` | `NOT_TESTED` |
| 5 | Replay a consumer group | **0** *within 24h* | ~10 min | offset reset — **consumer-side only** | **local ✔** |
| 6 | L1 lost | **24h** | ~30 min | rebuild from the Kafka window; beyond it, re-snapshot | `NOT_TESTED` |
| 7 | L2 corrupted | **0** | ~20 min | re-derive from L1; MERGE idempotent on `event_id` | **local ✔** |
| 8 | L3 wrong | **0** | ~10 min | rebuild by date; overwrite-by-filter | **local ✔** |
| 9 | Late event after snapshot | **0** | 1 rebuild | rebuild the date — this is the designed path | **local ✔** |
| 10 | Incompatible schema | **0** | ~10 min | registry rejects it; fix and redeploy the connector | `NOT_TESTED` |
| 11 | **Streaming down > 24h** | **24h** | hours | **events past retention are GONE** — re-snapshot | `NOT_TESTED` |
| 12 | Airflow metadata lost | run history only | ~1h | DAGs are code in Git; restore the PVC export | `NOT_TESTED` |
| 13 | Terraform state lost | infra definition | hours | S3 backend versioning; re-import | `NOT_TESTED` |

**"local ✔"** means the recovery *property* was executed against real Spark and Iceberg with
no AWS — see `artifacts/validation/session-15/drills/local-recovery-evidence.txt`. It does
**not** mean the operational procedure was rehearsed on deployed infrastructure.

## 3. Why the recoveries are cheap

Every layer is derivable from the one before it:

```
Kafka ──> L1 ──> L2 ──> L3 ──> Kimball ──> marts
```

- L2 is a pure function of L1 (idempotent on `event_id`)
- L3 is a pure function of L2 plus a cutoff (overwrite-by-filter per `snapshot_date`)
- marts are a pure function of L3

So recovery is **re-derivation, not restoration**. There are no data backups to restore for
L2 upward, because restoring a backup would be strictly worse than recomputing from the
layer below — a backup can be stale; a recomputation cannot.

This is also why `CLAUDE.md` §5 forbids deleting L1 before L2 is validated, and why the
registry marks L1 `deletion_blocked_until: l2_validated`. **L1 retention is a correctness
setting, not a storage setting.**

## 4. What IS backed up

| Artefact | Method | Contains secrets? |
|---|---|---|
| Connector configs | Git, with `${ssm:...}` references | **No** — credentials resolved at runtime |
| Registry schemas | exported to S3 by the EOD DAG | No |
| Airflow metadata | `pg_dump` → S3 before destroy | **Yes** — connections; encrypted with KMS |
| Terraform state | S3 backend, versioned + locked | **Yes** — treated as sensitive |
| Docs / DAGs / SQL | Git | No |

Acceptance criterion: *backup artifacts contain no secrets.* Connector configs satisfy this
by construction — they carry `${ssm:/path}` references, never literal passwords
(`CLAUDE.md` §3.1). The two that *do* contain sensitive material are encrypted and
access-restricted rather than pretended otherwise.

## 5. What this lab deliberately does not do

The session says: do not build expensive HA infrastructure for a lab. Accepted, and the
consequences are stated rather than hidden.

| Not built | Consequence | What it would cost |
|---|---|---|
| Multi-AZ Airflow | single k3s node = single point of failure for orchestration | a second node, ~$77/mo |
| MSK multi-region | region loss = total loss | roughly double MSK |
| Standby EMR | recovery waits for a cold start | pre-initialised capacity, always-on |
| Cross-region S3 replication | region loss = lake loss | ~2× storage + transfer |
| Automated failover | every recovery is manual | orchestration to build and maintain |

**The honest summary:** this lab survives *component* failure well — every layer is
re-derivable and every write is idempotent — and does not survive *region* failure at all.
For a portfolio project on a $30 budget that is the right trade; for production handling
real customer money it would not be.

## 6. Drills

```bash
scripts/failure-drill.sh list
scripts/failure-drill.sh run 8              # dry-run
scripts/failure-drill.sh run all --execute  # requires deployment
```

**No destructive action is ever taken against managed MSK.** Drills 5 and 6 exercise
consumer-side replay — resetting a consumer group offset, which is reversible — rather than
deleting topics, forcing leader elections or terminating brokers. MSK holds the only copy of
in-flight events and retention is 24h; destroying part of it to *test* resilience risks the
real data loss the drill is meant to prevent. AWS already tests broker failover.

Every drill writes evidence to `artifacts/validation/session-15/drills/`, including the ones
recorded `NOT_RUN` — a drill that could not run is a recorded fact, not a gap in the file.

## 7. Status

Nothing is deployed. Ten drills executed in dry-run and produced evidence files; four
recovery properties were demonstrated against real Spark and Iceberg. **No operational
recovery has been rehearsed on live infrastructure**, and no RTO figure here has been
measured — they are estimates from the architecture, and the first real drill is when they
stop being estimates.
