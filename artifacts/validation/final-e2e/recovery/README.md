# artifacts/validation/final-e2e/recovery

Failure / recovery / idempotency drills, 2026-09-03.
Document: `docs/validation/FAILURE_RECOVERY_MATRIX.md`.

| File | What |
|---|---|
| `01-offsets-before.txt` | Kafka offsets before the Connect restart |
| `02-connect-restart.txt` | restart, REST recovery timing, connector states |
| `03-after-restart.txt` | tasks settled RUNNING; offsets identical |
| `04-replay-after-restart.txt` | FULL_CDC replay: 5,744 in, 5,744 total — no duplicates |

## Headline

Connect restart: **RPO 0, RTO ~30 s** (documented ~2 min). Offsets byte-identical before
and after; the Oracle task passed briefly through `UNASSIGNED` and settled to `RUNNING`.
A follow-up FULL_CDC replay re-read all 5,744 events and the table stayed at 5,744.

Both global invariants held across every case observed in this window:
no FAILED execution advanced a watermark, and no retry duplicated final business state.
