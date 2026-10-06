# DATAHUB — SECURITY

- Phase: **DRP2**. Controls marked **enforced** are in code and tested; controls marked
  **designed** apply to a production deployment that does not exist.
- Related: `DATAHUB_ARCHITECTURE.md`, `CLAUDE.md` §3, ADR-087, ADR-089

---

## 1. The thing that is easy to miss

A metadata plane holds the **map of where every regulated column lives**. It is not
low-sensitivity infrastructure just because it holds no rows:

> The catalogue is treated at the classification of the most sensitive thing it describes.

Under this platform's scheme (`governance/registry/domains.yaml`) that is `restricted`.
Anyone who can read the catalogue can see which tables hold direct identifiers, which
columns are financial, and which marts are certified — which is exactly the reconnaissance
step before an attempt on the data itself.

---

## 2. No credential in Git — enforced

`cdc/metadata_plane.py` scans the loaded config and **refuses** any key whose name looks
like a credential and whose value is a literal:

```
auth.token: eyJhbGciOiJIUzI1NiJ9…
  -> REFUSING: a credential may not appear in this file. Name the environment variable
     (`token_env`) or the SSM parameter (`token_ssm_parameter`) instead.
```

Keys that name *where* a secret lives are allowed — `_env`, `_ssm_parameter`, `_source`,
`_arn`, `_path`, `_uri`, `_key_id`, `_provider`. A config that documents its secret source
is the goal; a config that contains the secret is the failure.

| Control | State |
|---|---|
| inline credential refused at load | **enforced**, tested |
| token read from `DATAHUB_GMS_TOKEN` at **call time**, never stored on the config object | **enforced**, tested |
| token absent from `repr()` of the config | **enforced**, tested |
| config file contains no `bearer `, JWT prefix, `password:` or AWS key literal | **enforced**, tested |
| production token from SSM SecureString, injected as an env var | **designed** |
| never a Terraform output in plaintext (CLAUDE.md §3.6) | **designed** |

The token is deliberately *not* held on the frozen config dataclass. Holding it would put it
into every `repr`, every log line that prints the config, and every pytest failure dump.

---

## 3. Network exposure

| Mode | Exposure |
|---|---|
| `disabled` | none — nothing runs |
| `local` | binds `127.0.0.1` only. No published port reaches a network interface; no inbound security-group rule exists |
| `production` | **private only**. No `0.0.0.0/0` to GMS, the frontend, Elasticsearch or the metadata store. Reached through SSM Session Manager port-forwarding — no SSH, no key pair, no public load balancer (CLAUDE.md §3.3, §3.4) |

Mode `production` **refuses to load** unless `networking: private_only`, `tls: required`,
`authentication: required` and `secret_source: ssm_securestring`. Those are not defaults that
can be edited down; they are load-time assertions.

---

## 4. Identity and least privilege

| Principal | Access | Notes |
|---|---|---|
| the publish client | write metadata to GMS | no AWS permission of its own |
| DataHub Glue / Athena ingestion (DRP4) | **read-only** Glue and Athena metadata | IRSA / instance profile; **no IAM user, no access key** (CLAUDE.md §3.2) |
| DataHub source-DB ingestion (DRP4) | metadata only, least privilege | see §6 |
| the frontend | authenticated; no anonymous catalogue read | **designed** |

**DataHub has no path to the source databases' data and no path to the executor.** It cannot
`SELECT` business rows, and it cannot start a job.

---

## 5. Metadata is untrusted execution input

The rule that matters most once impact analysis exists:

> **An asset name from the catalogue never becomes a shell command or a SQL string.**

An impact plan resolves each affected node to a **pre-registered executable job id**, through
the OPS job registry and the Airflow DAG registry. Anything that does not resolve becomes an
operator decision, never an improvised command. Concretely:

- `RecoveryPlan` refuses an `executable_descendants` entry whose asset kind is not executable —
  a Power BI report may be *impacted*, and there is no job to run for it. **enforced**, tested.
- `executable_descendants` must be a subset of `candidate_descendants`; anything else means
  the planner invented a target. **enforced**, tested.
- Every id in a plan is parsed as an `AssetId` (`kind:name`) before use, and a name containing
  the URN delimiters `(`, `)` or `,` is refused. **enforced**, tested.

Without this, a tag edited in a web UI becomes a way to make the platform rewrite data.

---

## 6. No PII in metadata

| Rule | State |
|---|---|
| PII **column names and categories** are published; **values never are** | designed; DRP4 recipes must not enable value profiling on sensitive tables |
| DQ results carry a `sample_reference`, validated as a **location** (`s3://`, `glue_catalog.db.table`, `quarantine://`) — never a row | **enforced**, tested |
| failing rows live in the access-controlled quarantine, not in the DQ ledger | **enforced** by the model |
| lineage facets carry ids, snapshots, counts and config versions — never payloads | designed (DRP3) |
| `business_keys` on an incident are ids, never personal values | designed |

The DQ table is the most widely read object in a lakehouse — dashboards, alerts, the AI
assistant, anyone debugging. Copying failing rows into it is how a restricted column ends up
in an unrestricted table, with a perfectly good reason at the time.

---

## 7. Environment isolation as a security control

A dev process emitting a `PROD` URN is refused (`assert_fabric`). This reads as a data-quality
control and is also a security one: without it, anyone with dev access can write assertions,
ownership and lineage onto production assets — silently, and with no way for a reader of the
production graph to tell.

Two environments may not share a fabric, and a client and its minter may not disagree about
which environment they speak for. Both are load-time refusals.

---

## 8. Supply chain

| Control | State |
|---|---|
| server image pinned to an exact tag, **digests recorded** for gms / frontend / actions | **enforced** |
| CLI pinned to an exact version | **enforced** |
| floating tags (`latest`, `nightly`, `head`, `quickstart`) refused at load | **enforced**, tested |
| server and CLI must share a release line | **enforced**, tested |
| the client adds **no new Python dependency** — `urllib` only | **enforced** |

The last one is a supply-chain decision as much as a deployment one: this module is shipped
to EMR inside `cdc-framework.zip`, and adding an HTTP client to publish metadata would put a
new transitive dependency tree into every Spark job.

---

## 9. Availability is a safety property here

The metadata plane being down must never corrupt or block business data. That is enforced
by the outage policy (`BEST_EFFORT` for every data flow, and the config **refuses** to mark a
data flow `REQUIRED`), and by bounded retry with a stateable worst case of 31.5 s.

The inverse also holds: when the plane is down, work that *depends* on it fails safely rather
than guessing. `lineage_impact` is `REQUIRED` precisely so that a recovery is never planned
on a blast radius nobody could read.

---

## 10. Not yet covered

| | Owner |
|---|---|
| credential rotation procedure for a live GMS token | DRP2-prod |
| authentication provider choice (OIDC vs JAAS) and group mapping | DRP2-prod |
| audit logging of catalogue reads | DRP10 |
| source-DB ingestion least-privilege grants, written out per engine | DRP4 |
| Power BI service-principal scope | DRP4 |
