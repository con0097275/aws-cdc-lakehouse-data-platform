# METADATA SECURITY

- Phase: DRP10 · Complements `DATAHUB_SECURITY.md` (the service) with the *data* in it
- Related: ADR-089, ADR-090, `CLAUDE.md` §3

---

## 1. The claim that matters

> **Metadata is untrusted execution input.**

An impact plan is computed from a graph that ingestion wrote. If an asset name from that
graph can become a shell command or a SQL string, then anyone who can edit a tag in a web UI
can make the platform rewrite data.

Enforced, tested, in three places:

1. `RecoveryPlan` refuses an `executable_descendants` entry whose kind is not executable — a
   Power BI report may be *impacted*, and there is no job to run for it.
2. `executable_descendants` must be a **subset** of `candidate_descendants`; anything else
   means the planner invented a target.
3. `recovery_coordinator` maps a job name to `JOB_ENTRYPOINTS` — a **closed** dict — and
   refuses anything else. Every id is parsed as an `AssetId` before use, and a name containing
   the URN delimiters `(`, `)` or `,` is refused.

`LineageImpactService` additionally downgrades a node to `UNKNOWN` when no *registered* job
produces it. `UNKNOWN` is a real answer: not a reason to improvise a command, a reason to ask
a person.

## 2. No value ever crosses the boundary

| Control | Where | State |
|---|---|---|
| ingestion profiling off on every source | the recipes | enforced, tested |
| DQ `sample_reference` must be a location | `cdc/quality.py` | enforced, tested |
| lineage facet allowlist, deny-guarded **at config load** | `cdc/lineage_runtime.py` | enforced, tested |
| `spark.openlineage.facets.custom_environment_variables=[]` | the derived Spark conf | enforced |
| `AIRFLOW__OPENLINEAGE__DISABLE_SOURCE_CODE=true` | the Helm values | enforced |
| assertion runs carry a verdict + OPS run id, never evidence | `dq_catalog.assertion_mcps` | enforced, tested |

The deny list guards the **allowlist**, not the facets directly — otherwise a data-carrying
name could be added to the allowlist and published forever, reviewed-looking and never
re-examined.

An early version denied the bare substring `row` and rejected `rows_read`, which is a
**count**. The rule is about values, not nouns; that is why the layering is what it is.

## 3. No credential anywhere

- `cdc/metadata_plane.py` **refuses at load** any key that looks like a credential and holds
  a literal. Keys naming *where* a secret lives (`_env`, `_ssm_parameter`, `_source`,
  `_arn`, …) are allowed — a config that documents its secret source is the goal.
- The token is read from `DATAHUB_GMS_TOKEN` **at call time** and never stored on the config
  object, so it cannot leak through a `repr`, a log line or a pytest failure dump.
- Every recipe references `${VAR}`, never a value. A test asserts no recipe contains a JWT
  prefix, an `AKIA` key or a bearer literal.
- No IAM user, no access key. Ingestion authenticates through IRSA / the instance profile.

## 4. DataHub cannot reach the sources or the executor

| | |
|---|---|
| source databases | metadata only, least privilege, profiling off. No path to business rows. |
| the executor | none. Airflow alone runs jobs; the planner hands it a plan, not a command. |
| AWS | read-only Glue and Athena metadata through IRSA. |

## 5. Environment isolation is a security control

Without `assert_fabric()`, anyone with dev access could write ownership, tags, assertions and
lineage onto **production** assets — silently, and with no way for a reader of the production
graph to tell. Two environments may not share a fabric; a client and its minter may not
disagree about which environment they speak for. Both are load-time refusals.

## 6. The catalogue is itself sensitive

It holds the map of where every regulated column lives. Treated at the classification of the
most sensitive thing it describes (`restricted` under this platform's scheme), which is why
`production` mode refuses to load without private networking, TLS, authentication and an SSM
secret source.

## 7. Open

| # | Item | Owner |
|---|---|---|
| 1 | audit logging of catalogue reads | DRP10-prod |
| 2 | credential rotation for a live GMS token | DRP2-prod |
| 3 | authentication provider and group mapping | DRP2-prod |
| 4 | no asset is classified `restricted`, though FULL_CDC carries raw payloads | governance decision |
