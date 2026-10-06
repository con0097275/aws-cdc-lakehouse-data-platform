# Runbook — Iceberg rollback and time travel

**When:** a bad commit reached a table.

## 1. A failed commit needs no rollback

Iceberg commits are **atomic**. A `CommitFailedException` leaves the table at its previous
snapshot — there is no partial state to clean up. Confirm before acting:

```sql
SELECT snapshot_id, committed_at, operation, summary
FROM "<db>"."<table>$snapshots" ORDER BY committed_at DESC LIMIT 10;
```

If the bad run's timestamp has no snapshot, nothing landed. Stop here.

## 2. Read before you roll back

```sql
SELECT * FROM <db>.<table> VERSION AS OF <snapshot_id> WHERE business_date = DATE '<d>';
```

Time travel answers "what did it look like" **without changing anything**. Prefer it —
most investigations end here.

## 3. Prefer a rerun to a rollback

A rollback reverts **the whole table**, including good rows other modes wrote after the bad
commit. A rerun of the affected date is scoped to that date and idempotent on the business
key. Reach for rollback only when a rerun cannot fix it.

## 4. Rollback

```sql
CALL glue_catalog.system.rollback_to_snapshot('<db>.<table>', <snapshot_id>);
```

Then **re-establish the tier**: rolling back does not undo the watermark. Check the
watermark still names an execution consistent with the table
([watermark-investigation.md](watermark-investigation.md)) and rerun the date if not.

## 5. EOD tags are the safe anchors

`EOD_<date>` tags pin the exact snapshot the gate certified — that is what they are for.
Rolling back to a tag returns to a state that was validated, rather than to an arbitrary
commit.

## 6. Retention limits how far back you can go

`expire_snapshots` removes old snapshots, and orphan cleanup removes their files. Retention
must exceed the longest window any job might still commit into, or cleanup races a live
writer. If a snapshot has expired, time travel to it will fail — the tag is not a backup.
