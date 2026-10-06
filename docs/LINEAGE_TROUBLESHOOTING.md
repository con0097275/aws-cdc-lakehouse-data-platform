# LINEAGE TROUBLESHOOTING

- Phase: DRP6 · Companions: `END_TO_END_LINEAGE.md`, `LINEAGE_NAMING_STANDARD.md`

---

## 0. First, rebuild the graph and audit it

```bash
python3 -c "import sys; sys.path.insert(0,'.'); \
  from cdc.lineage_graph import build_static_graph, audit; \
  from cdc.urns import UrnMinter, default_catalog; \
  from cdc.metadata_plane import load; \
  g = build_static_graph(UrnMinter(load().environment, default_catalog())); \
  print(len(g.nodes), 'nodes', len(g.edges), 'edges', len(g.cycles()), 'cycles'); \
  [print(' ', f) for f in audit(g)]"
```

Offline, sub-second, no AWS and no DataHub. If this is clean the problem is in emission or
ingestion, not in the model.

---

## 1. "The impact query returns nothing"

| Cause | Check | Fix |
|---|---|---|
| the root is not in the graph | `root in g.nodes` | the asset has no declared producer — add it to `lineage_declared.yaml` or its config |
| the root is spelled differently | `minter.asset_for(urn)` round-trips | use `UrnMinter`, never a formatted string |
| wrong fabric | the URN ends `,DEV)` | a `PROD` URN in a dev process is refused on emit; on read it simply matches nothing |
| `max_hops` too small | raise it | but see §6 — an unbounded radius is its own problem |

`downstream()` on an unknown root returns an **empty result with the root recorded**, not an
exception. The catalogue not knowing an asset is itself the finding.

## 2. "One table appears twice"

The symptom of the failure ADR-089 exists to prevent. Run `audit()` — `duplicate_canonical_asset`
names it.

Causes, in order of likelihood:

1. an ingestion recipe naming a lake table on a platform other than `glue`
   (`assert_no_duplicate_identity()` refuses this at compile — check it was called)
2. an OpenLineage dataset emitted without its `datahub_urn` facet, so the backend inferred
   a name (ADR-090: identity is carried, never inferred)
3. a hand-formatted URN somewhere

## 3. "The chain stops at the CURATED layer"

Expected before DRP6: the Kimball facts are built in Python and declared nowhere. They are
now in `governance/registry/lineage_declared.yaml`. If a *new* fact appears and the chain
breaks again, add it there — and the fix is `declared`, so mart recoveries downstream of it
stay operator-approved until the builders read a config contract.

## 4. "Nothing is `observed`"

Correct today. `governance/registry/openlineage.yaml` has `enabled: false`, the Airflow
provider is not installed, and no Spark job has run with `OPENLINEAGE=1`. Check in order:

```bash
grep -n '^enabled' governance/registry/openlineage.yaml           # false?
OPENLINEAGE=1 bash scripts/emr-submit.sh ...                      # opt-in flag set?
# Airflow: AIRFLOW__OPENLINEAGE__DISABLED must be "false" AND the provider installed
```

Note `governance/lineage/openlineage.yml` (the Session-14 file) still marks the Spark hops
`evidence: observed`. **That claim is false** and is left in place deliberately until DRP6
replaces the file with emitted edges.

## 5. "A recovery says REQUIRES_APPROVAL and I expected AUTOMATIC"

Read the reasons — `decide_approval()` returns **all** of them. The most common today:

> *the weakest lineage edge on this path is declared; automatic recovery may only traverse
> derived, observed*

That is working as designed. Every mart is downstream of a Python-only fact producer, so
every mart recovery is operator-approved. Closing it means giving the fact builders a config
contract, not lowering the gate.

## 5b. "The traversal stops short / the blast radius looks too small"

**DataHub's graph index is eventually consistent.** Aspects are stored synchronously; the
relationship index is populated by an asynchronous consumer.

Measured on 2026-09-30, immediately after publishing 67 `upstreamLineage` aspects:

| When | Assets reached from `src:oracle.coredb.corebank.account` |
|---|---|
| seconds after the publish | **3** — the chain stopped at EOD |
| minutes later | **18** — the mart reached at hop 7 |

The stored aspects were correct the whole time; only the index lagged.

> **This is the dangerous direction.** A traversal run too early does not fail — it returns
> a smaller blast radius, confidently. A recovery planned on it would leave descendants
> un-rebuilt and look complete.

Rules that follow:

1. **Never compute an impact plan straight after an ingestion run.** Wait for the index, or
   read the stored `upstreamLineage` aspects directly.
2. Cross-check a suspicious traversal against `get_aspect(urn, "upstreamLineage")`, which is
   synchronous.
3. `LineageImpactService` today reads the **offline** `LineageGraph`, built from config and
   artefacts, so it is not exposed to this. When DRP8 is moved onto the DataHub API it must
   handle it explicitly — a smaller answer must never be mistaken for a smaller problem.

## 6. "The blast radius is the whole warehouse"

Either `max_hops` is too large, or an edge exists that should not. Check for a cycle first
(`g.cycles()`), then look at the path: `g.path(root, surprising_node)` shows the route, and
a surprising route is usually a shared dimension — `dim_customer` legitimately reaches almost
everything.

That is not a bug; it is why `max_blast_radius` exists and why a large radius requires
approval.

## 7. "Two jobs write one table"

`producing_job()` raises rather than choosing:

> *X is written by more than one job (a, b). A rerun would have to choose, and choosing
> silently rebuilds a table with the wrong producer.*

Fix the graph — usually a declared edge whose `job` is wrong.

## 8. "A column trace ends early"

Column edges exist for the conformance step (`derived`) and dbt same-name mappings
(`declared`). They do **not** exist through `spark/facts/`. `_column_reaches()` falls back to
dataset scope where no column edge is known, so impact stays correct — it just stops being
narrow. See `COLUMN_LINEAGE_STRATEGY.md` §7.

## 9. Quick reference

| Symptom | First command |
|---|---|
| anything at all | the audit snippet in §0 |
| a path question | `g.path(a, b)` then `g.weakest_evidence(path)` |
| "who writes this?" | `svc.producing_job(urn)` |
| "what breaks if I change this?" | `svc.downstream(urn, max_hops=6).by_class()` |
| "why approval?" | `svc.plan(...)` and read every reason |
| "which run produced this?" | `g.runs_for(urn)` |
| "where did this column come from?" | `g.column_upstreams(urn, "col")` |
