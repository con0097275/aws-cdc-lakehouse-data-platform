# Runbook — STREAMING_RT restart

**When:** the streaming application crashed, stalled, or must be cycled.

⚠️ This is the only workload that bills **per hour** rather than per run. Check it is meant
to be running before you restart it.

## 1. Is it enabled at all?

`is_enabled: false` in the job YAML ships it off. Turning it on takes **two** deliberate
acts — that flag and the `ENABLE_STREAMING_RT` gate — precisely so it cannot be enabled by
accident.

## 2. Crash vs restart — they are different records

- **`record_streaming_error`** — a batch failed, the app did not restart. `restart_count`
  is **unchanged**.
- **`record_streaming_restart`** — the app restarted. `restart_count` increments.

Conflating them makes a crash-looping stream look like a healthy one that happens to have
restarted a lot. If you see errors climbing and `restart_count` flat, it is failing without
recovering.

## 3. Restart

```bash
python3 scripts/reporting-live-run.py --flow-mode STREAMING_RT --business-date <d> \
  --coordinator-run-id "rt-$(date +%s)" --evidence /tmp/rt.json --execute
```

A restart keeps the **same deployment id** (the code did not change) and the **same
checkpoint** — which is what makes it resume rather than replay from the beginning.

## 4. Verify it resumed, not replayed

`checkpoint_state_at_start` should report committed batches. `EMPTY` after a restart means
it is starting from scratch — investigate before letting it run, because it will re-read
from the stream's configured start position.

## Do NOT delete the checkpoint

See [checkpoint-recovery.md](checkpoint-recovery.md). There is no `delete()` on
`CheckpointInspector` and there must not be one.
