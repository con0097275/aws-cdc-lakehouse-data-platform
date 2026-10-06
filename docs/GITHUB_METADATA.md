# GitHub repository metadata

Copy-paste values for the repository's About panel and social preview. Kept in the repo so
the landing-page wording and the GitHub metadata cannot drift apart.

## Description (350 char limit; this is 148)

```text
Production-oriented AWS CDC lakehouse: Oracle/SQL Server → Debezium → Kafka → Spark → Iceberg, with DataHub/OpenLineage governance and AI-assisted bounded recovery.
```

## Website

```text
(leave blank — there is no hosted demo, and a dead link is worse than none)
```

## Topics

Only topics that match what is actually in this repository:

```text
data-engineering
data-platform
lakehouse
aws
cdc
change-data-capture
debezium
apache-kafka
apache-spark
apache-iceberg
airflow
dbt
terraform
data-governance
data-lineage
data-quality
datahub
openlineage
langgraph
```

Deliberately **not** used: `big-data`, `etl`, `machine-learning`, `llm`, `agents`,
`production` — each either overstates what is here or attracts the wrong reader.

Applying them, once `gh auth login` has been run:

```bash
gh repo edit --description "Production-oriented AWS CDC lakehouse: Oracle/SQL Server → Debezium → Kafka → Spark → Iceberg, with DataHub/OpenLineage governance and AI-assisted bounded recovery."
gh repo edit --add-topic data-engineering,data-platform,lakehouse,aws,cdc,change-data-capture,debezium,apache-kafka,apache-spark,apache-iceberg,airflow,dbt,terraform,data-governance,data-lineage,data-quality,datahub,openlineage,langgraph
```

## Social preview image (1280×640)

GitHub crops to 1280×640 and renders it small in link previews, so it has to survive being
read at a glance. Specification rather than an asset, because a generated image that looks
auto-generated is worse than the default:

| | |
|---|---|
| Background | Solid dark (`#0f1419`) — matches the copilot UI in the screenshots |
| Line 1 | **AWS CDC Lakehouse** — large, weight 700 |
| Line 2 | Kafka · Spark · Iceberg · Airflow · dbt — medium, muted (`#8a94a6`) |
| Line 3 | Data Governance · Lineage · AI-assisted recovery — medium, muted |
| Bottom-right | `3,529 tests · ~$1.40/day` — small, muted |
| Avoid | Logos of AWS/Apache projects (trademark), screenshots (illegible at preview size), gradients |

Keep the safe area inside a 64 px margin: GitHub crops the edges in some surfaces.

## Repository settings worth checking

- **Issues**: on — an interviewer asking a question in an issue is a good outcome.
- **Discussions / Wiki / Projects**: off — nothing maintains them.
- **Releases**: none. This is not a versioned artifact.
- **Default branch**: see the note in [`../PUBLIC_RELEASE_REPORT.md`](../PUBLIC_RELEASE_REPORT.md) — this copy was committed on `master`.
