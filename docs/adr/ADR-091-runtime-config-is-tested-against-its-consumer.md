# ADR-091 — Runtime configuration is tested against its consumer, not against a sibling file

* **Status:** Accepted
* **Date:** 2026-09-30
* **Phase:** DRP3 / DRP12
* **Related:** ADR-087, ADR-090

## Context

`cdc/lineage_runtime.py` derives the Airflow provider's environment, and
`airflow/helm/values.yaml` carries it into the deployment. A test asserted the two matched:

```python
for k, v in em.airflow_env().items():
    assert env.get(k) == v
```

It passed for the whole of DRP3. On 2026-09-30 the provider was installed into a pod running
the deployed `apache/airflow:3.2.2` image, and emitted **nothing**.

`AIRFLOW__OPENLINEAGE__TRANSPORT` was the bare string `console`. The provider reads that one
key as JSON:

```python
conf.getjson("openlineage", "transport", fallback={})
```

so it raised `AirflowConfigException: Unable to parse [openlineage] 'transport' as valid
json` — **during plugin import**. Airflow logged one line, skipped the plugin, registered no
listener, and ran the DAG green:

```
REGISTERED_LISTENERS = []      # three tasks succeeded; zero events
```

The bare form looked right because the *Spark* listener genuinely takes a flat
`spark.openlineage.transport.type=console` string. Two integrations, two encodings, one
config field.

The test could not have caught this. It compared two files in this repository against each
other. Both carried the bare string, so both agreed — and agreement between two artefacts
the same author wrote says nothing about the third party that has to parse them. The test
was measuring internal consistency and reporting it as correctness.

## Decision

**A test over runtime configuration must assert the consumer's contract, not a sibling
file's agreement.** Concretely, for every configuration value this repository emits into a
third-party runtime:

1. The test parses or validates the value **the way the consumer does** — here,
   `json.loads` on the transport value, plus an assertion that the result is an object with
   a `type`. A test may keep the file-to-file comparison, but never as the only assertion.
2. Where the encoding differs between two consumers of the same logical setting, the
   deriving function owns both encodings and names the difference in a comment. It is not
   left to whoever edits the values file.
3. A value that cannot be expressed safely is **refused, not half-rendered**.
   `airflow_transport()` raises for `http` rather than emitting an object without its token,
   because the OpenLineage client does not expand `${VAR}` and the only working literal
   would put a secret into `values.yaml` and into the Helm release secret, against security
   invariant 6. The refusal names `AIRFLOW__OPENLINEAGE__TRANSPORT_CMD` as the route that
   does work.

## Options

| | Option | Why not |
|---|---|---|
| A | Fix the one value and leave the test as it was | The value would be right and the test would still be measuring the wrong thing. The next config that has to cross into a third-party runtime fails the same way. |
| B | Assert the consumer's contract in the test (**chosen**) | Catches this defect and its siblings without needing the runtime present in CI. |
| C | Require a live runtime check in CI for every config value | Correct and unaffordable here: it means an Airflow, a Spark and a DataHub in CI. Kept as the manual step that actually found this. |
| D | Drop the file-to-file comparison | It still has value — it catches `values.yaml` drifting away from the deriving module. Kept, but no longer alone. |

## Consequences

- Three tests replace one. They fail on the exact defect that shipped.
- The class of bug is narrowed but not eliminated: a test can still encode a wrong belief
  about a consumer. The durable defence is the one that found this — run it against the real
  runtime and check that the *effect* occurred, not that the config was written.
- This generalises beyond lineage. The same shape — two internal files agreeing, a silent
  third-party rejection — applies to Spark conf, connector JSON, Helm values and dbt
  profiles. Where those are asserted only against each other, the assertion is weaker than
  it appears.

## Cost

$0. No AWS resource is created, changed or left running. The live verification used one
short-lived pod on the already-running Airflow node, deleted afterwards; all eight
production pods stayed `Running` throughout.

## Security

The refusal in decision point 3 is the security-relevant part. Emitting an `http` transport
inline would require the DataHub API token as a literal in `values.yaml` and therefore in
the Helm release secret, breaking security invariant 6 (secrets come from Secrets
Manager/SSM at runtime, never from a file in Git). `airflow_transport()` raises instead, and
a test asserts the refusal names both the token variable and the `_CMD` route. No token,
URL or credential appears in the derived environment — only the *names* of the variables
that carry them.

## Rollback

Revert `cdc/lineage_runtime.py` and the `AIRFLOW__OPENLINEAGE__TRANSPORT` value in
`airflow/helm/values.yaml`. Nothing is deployed, because the provider is not in the Airflow
image and `AIRFLOW__OPENLINEAGE__DISABLED` is still `true`: the value is inert in the
running cluster. There is no state to unwind and no migration to reverse.

## Validation

| | Result |
|---|---|
| `spark/tests/test_drp3_openlineage.py` | 66 passed, including the contract and classpath-guard tests |
| full suite | 3,301 passed, 0 failed |
| live, before the fix | `REGISTERED_LISTENERS = []`, 0 events, DAG run `success` |
| live, after the fix | listener registered, **7 events**, namespace `cdc-lakehouse-dev`, `parent` run facet present |
| evidence | `artifacts/validation/data-reliability/drp3-airflow-runtime-lineage.json` |

The before/after pair is the validation that matters: the same DAG, the same image, the same
provider, one character class different in one environment variable.

## The failure mode worth naming

A misconfiguration that fails loudly costs an afternoon. This one **succeeded at everything
except the thing it was for**: the pipeline ran, the tasks passed, the dashboards were
green, and the only signal was an absence — no events, somewhere nobody was counting. The
platform's own rule already covered it, and the rule is now enforced by tests rather than
remembered:

> A test that asserts on generated text cannot see that the text does not run.
