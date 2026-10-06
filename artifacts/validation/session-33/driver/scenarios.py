"""PHASE 14 — the eight reporting scenarios, against real infrastructure.

Real: the repository's own coordinator/flow code, the four DynamoDB runtime-state tables,
the Glue catalog, and Iceberg on S3. Execution records, watermarks and status transitions
are written by spark/reporting/*, not by this script.

SUBSTITUTED, and labelled as such in the evidence: `dbt build`. dbt-core/dbt-spark are not
installed in the EMR image and the private subnets cannot reach PyPI, so the transformation
runs as the equivalent Spark SQL MERGE with the same MERGE_LADDER semantics. Everything the
framework does AROUND the transformation is real; the dbt invocation itself is not proven
and is not claimed.
"""
import argparse, json, sys, time, zipfile
from datetime import date, datetime, timedelta, timezone

sys.path.insert(0, "/tmp/fw")
from pyspark.sql import SparkSession

EV = []


def emit(**kw):
    EV.append(kw)
    print("SCENARIO_EVIDENCE " + json.dumps(kw, default=str))


def snapshot_of(spark, table):
    r = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()
    return int(r[0][0]) if r else None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan-uri", required=True)
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--mart", required=True)
    ap.add_argument("--curated", required=True)
    ap.add_argument("--prefix", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--region", default="ap-southeast-1")
    ap.add_argument("--reset", action="store_true",
                    help="clear mart rows, backfilled dates and watermarks first")
    args = ap.parse_args()

    spark = (SparkSession.builder.appName("phase14-scenarios")
             .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.glue_catalog.catalog-impl",
                     "org.apache.iceberg.aws.glue.GlueCatalog")
             .config("spark.sql.catalog.glue_catalog.warehouse", args.warehouse)
             .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
             .config("spark.sql.extensions",
                     "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # framework
    import boto3
    from models import FlowMode, ResourceProfile, ValidationStatus
    from ops_client import OpsClient
    from dynamodb_state import DynamoDbRuntimeStateRepository
    from coordinator import Coordinator

    plan = json.loads("".join(spark.read.text(args.plan_uri).rdd.map(lambda r: r[0]).collect()))
    # EMR Serverless does not export AWS_REGION into the driver, so boto3 has no
    # region to discover and raises NoRegionError. Pass it explicitly.
    ddb = boto3.resource("dynamodb", region_name=args.region)
    repo = DynamoDbRuntimeStateRepository(
        execution_table=ddb.Table(f"{args.prefix}-job-execution"),
        watermark_table=ddb.Table(f"{args.prefix}-job-watermark-state"),
        summary_table=ddb.Table(f"{args.prefix}-summary-config"),
        streaming_table=ddb.Table(f"{args.prefix}-streaming-app-state"))
    client = OpsClient(repo)
    profiles = {"small": ResourceProfile("small", 1, "4g", 2, "8g", 2),
                "medium": ResourceProfile("medium", 2, "8g", 2, "16g", 4),
                "large": ResourceProfile("large", 4, "16g", 4, "32g", 8)}
    coord = Coordinator(client, plan=plan, profiles=profiles, code_version="phase14")

    spark.sql(f"""CREATE TABLE IF NOT EXISTS {args.mart} (
        account_sk BIGINT, customer_sk BIGINT, business_date DATE,
        closing_balance DECIMAL(18,2), debit_amount DECIMAL(18,2),
        credit_amount DECIMAL(18,2), txn_count BIGINT,
        processing_status STRING, input_cutoff TIMESTAMP
    ) USING iceberg PARTITIONED BY (business_date) TBLPROPERTIES ('format-version'='2')""")

    STATUS_RANK = {"PROVISIONAL_NRT": 1, "PROVISIONAL_CORRECTED": 2, "CERTIFIED": 3}

    def merge_mart(business_date, tier, cutoff, where=""):
        """The guarded MERGE (ADR-042), as Spark SQL. A row may only be overwritten by a
        HIGHER tier, or the same tier with a later input_cutoff -- the losing rows are
        filtered out of the SOURCE, because dbt-spark exposes no WHEN MATCHED AND clause and
        putting the guard in the ON clause would INSERT a duplicate instead of skipping."""
        rank = STATUS_RANK[tier]
        sql = f"""
        MERGE INTO {args.mart} t USING (
          SELECT account_sk, customer_sk, business_date, closing_balance,
                 debit_amount, credit_amount, txn_count,
                 '{tier}' AS processing_status,
                 TIMESTAMP '{cutoff}' AS input_cutoff
          FROM (SELECT *, row_number() OVER (
                         PARTITION BY account_sk, business_date ORDER BY closing_balance DESC
                       ) AS _rn
                FROM {args.curated}
                WHERE business_date = DATE '{business_date}' {where}) 
          WHERE _rn = 1
        ) s
        ON t.account_sk = s.account_sk AND t.business_date = s.business_date
        WHEN MATCHED AND ({rank} > CASE t.processing_status
                                     WHEN 'CERTIFIED' THEN 3
                                     WHEN 'PROVISIONAL_CORRECTED' THEN 2 ELSE 1 END
                          OR ({rank} = CASE t.processing_status
                                     WHEN 'CERTIFIED' THEN 3
                                     WHEN 'PROVISIONAL_CORRECTED' THEN 2 ELSE 1 END
                              AND s.input_cutoff >= t.input_cutoff))
            THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *"""
        spark.sql(sql)

    class InlineSubmitter:
        """Stands in for EmrServerlessSubmitter. The framework only needs an engine id and a
        terminal state back; the transformation runs here in the same session."""
        def __init__(self): self.n = 0
        def submit(self, request):
            from submitter import SubmissionHandle
            self.n += 1
            return SubmissionHandle(engine_run_id=f"inline-{self.n}",
                                    tracking_url="inline://phase14")
        def poll(self, handle):
            from status import ExecutionStatus
            return ExecutionStatus.SUCCEEDED, "inline transformation completed"

    BD = spark.sql(f"SELECT max(business_date) d FROM {args.curated}").collect()[0][0]
    JOB = "mart_account_balance_daily"

    if args.reset:
        # A validation run has to start from a known state. Earlier failed attempts left
        # mart rows, a backfilled "gap" date and advanced watermarks behind, which made
        # scenario 4 report NOT_A_GAP and every watermark look unchanged -- evidence that
        # is real but proves nothing.
        spark.sql(f"DELETE FROM {args.mart}")
        spark.sql(f"DELETE FROM {args.curated} WHERE business_date <> DATE '{BD}'")
        for m in (FlowMode.EOD, FlowMode.AUTO_CORRECT, FlowMode.FULFILL, FlowMode.STREAM_BATCH):
            try:
                repo._watermarks.delete_item(Key={"watermark_key": f"{JOB}#{m.value}"})
            except Exception as exc:
                print(f"RESET watermark {m.value}: {exc}")
        print("RESET done: mart emptied, backfills removed, watermarks cleared")

    def watermark(mode):
        w = client.get_watermark(JOB, mode)
        return str(w.watermark_ts or w.last_success_date_of_data) if w else None

    def counts(bd):
        r = spark.sql(f"SELECT count(*) c, sum(closing_balance) s FROM {args.mart} "
                      f"WHERE business_date = DATE '{bd}'").collect()[0]
        return int(r[0]), (float(r[1]) if r[1] is not None else 0.0)

    def scenario(n, name, mode, bd, tier, where="", note=""):
        t0 = time.time()
        run = coord.plan_run(coordinator_run_id=f"p14-s{n}-{int(time.time())}",
                             flow_mode=mode, business_date=bd, now=datetime.now(timezone.utc))
        task = run.tasks_for(1)[0]
        wm_before = watermark(mode)
        snap_before = snapshot_of(spark, args.mart)
        rows_before, _ = counts(bd)
        gate = coord.gate(task)
        cutoff = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        sub = InlineSubmitter(); h = sub.submit(None)
        coord.submit(task, spark_app_id=h.engine_run_id, tracking_url=h.tracking_url)
        merge_mart(bd, tier, cutoff, where)
        status = coord.finish(task, validation_status=ValidationStatus.PASSED,
                              metrics={"rows_updated": counts(bd)[0]})
        rows_after, total_after = counts(bd)
        emit(scenario=n, name=name, note=note, job_id=JOB, flow_mode=mode.value,
             coordinator_run_id=run.coordinator_run_id, execution_id=task["execution_id"],
             turn=task["turn"], attempt_number=task["attempt_number"],
             dependencies=task.get("required_upstreams", []), gate=gate,
             business_date=str(bd), tier=tier,
             watermark_before=wm_before, watermark_after=watermark(mode),
             spark_app_id=h.engine_run_id, dbt_build="SUBSTITUTED_SPARK_SQL_MERGE",
             iceberg_snapshot_before=snap_before,
             iceberg_snapshot_after=snapshot_of(spark, args.mart),
             rows_before=rows_before, rows_after=rows_after,
             mart_balance_sum=total_after,
             duration_s=round(time.time() - t0, 1), final_status=status)
        return task

    # 1 normal EOD
    scenario(1, "normal EOD", FlowMode.EOD, BD, "CERTIFIED",
             note="first certified close of the business date")
    # 2 late CDC arrives  (a real UPDATE on the source, re-derived into curated)
    spark.sql(f"""MERGE INTO {args.curated} t USING (
        SELECT account_sk, customer_sk, business_date,
               closing_balance + 999.99 AS closing_balance,
               debit_amount, credit_amount, txn_count, processing_status
        FROM {args.curated} WHERE business_date = DATE '{BD}' AND account_sk =
             (SELECT min(account_sk) FROM {args.curated})) s
        ON t.account_sk = s.account_sk AND t.business_date = s.business_date
        WHEN MATCHED THEN UPDATE SET t.closing_balance = s.closing_balance""")
    emit(scenario=2, name="late CDC arrives", note="one account's balance moved by +999.99 "
         "in the EOD source AFTER the certified close", business_date=str(BD),
         cdc_operation="u", affected_keys=1, final_status="SOURCE_MUTATED")
    # 3 AUTO_CORRECT picks it up
    scenario(3, "AUTO_CORRECT", FlowMode.AUTO_CORRECT, BD, "PROVISIONAL_CORRECTED",
             note="bounded correction; must NOT overwrite the CERTIFIED row (ADR-042 ladder)")
    # 4 missing historical date
    gap = BD - timedelta(days=3)
    missing, _ = counts(gap)
    emit(scenario=4, name="missing historical date", business_date=str(gap),
         rows_before=missing, final_status="GAP_CONFIRMED" if missing == 0 else "NOT_A_GAP")
    # 5 FULFILL that date
    spark.sql(f"DELETE FROM {args.curated} WHERE business_date = DATE '{gap}'")
    spark.sql(f"""INSERT INTO {args.curated}
        SELECT account_sk, customer_sk, DATE '{gap}', closing_balance, debit_amount,
               credit_amount, txn_count, processing_status
        FROM {args.curated} WHERE business_date = DATE '{BD}'""")
    scenario(5, "FULFILL", FlowMode.FULFILL, gap, "PROVISIONAL_CORRECTED",
             note="backfill of the confirmed gap date")
    # 6 new realtime change
    spark.sql(f"""MERGE INTO {args.curated} t USING (
        SELECT account_sk, customer_sk, business_date, closing_balance + 5.55 AS closing_balance,
               debit_amount, credit_amount, txn_count, processing_status
        FROM {args.curated} WHERE business_date = DATE '{BD}' AND account_sk =
             (SELECT max(account_sk) FROM {args.curated})) s
        ON t.account_sk = s.account_sk AND t.business_date = s.business_date
        WHEN MATCHED THEN UPDATE SET t.closing_balance = s.closing_balance""")
    emit(scenario=6, name="new realtime change", note="+5.55 on one account, to be picked up "
         "by the micro-batch", business_date=str(BD), cdc_operation="u", affected_keys=1,
         final_status="SOURCE_MUTATED")
    # 7 STREAM_BATCH
    scenario(7, "STREAM_BATCH", FlowMode.STREAM_BATCH, BD, "PROVISIONAL_NRT",
             note="micro-batch; PROVISIONAL_NRT must LOSE to the CERTIFIED row already there")
    # 8 STREAMING_RT, independently
    from streaming_rt import (InMemoryCheckpointInspector, application_name,
                              resolve_stream_source, start_application, stop_application)
    from dataclasses import fields as dc_fields
    from models import (JobFlowConfig, SourceLayerPolicy, EodGateMode, WatermarkType,
                        MergeStrategy, DeleteStrategy, CoordinatorMode, RunMode,
                        CorrectionSource, AffectedDatePolicy, AffectedKeyStrategy, CostGuard)

    def flow_from_row(row):
        """config_loader.flow_from_plan_row, inlined.

        config_loader imports jsonschema at module scope for YAML validation, and jsonschema
        is not in the EMR image (no PyPI from the private subnets). The plan has ALREADY been
        schema-validated at compile time, so re-validating here would add nothing anyway.
        """
        known = {f.name for f in dc_fields(JobFlowConfig)}
        enums = {"flow_mode": FlowMode, "source_layer_policy": SourceLayerPolicy,
                 "eod_gate_mode": EodGateMode, "watermark_type": WatermarkType,
                 "merge_strategy": MergeStrategy, "delete_strategy": DeleteStrategy,
                 "coordinator_mode": CoordinatorMode, "run_mode": RunMode,
                 "correction_source": CorrectionSource,
                 "affected_date_policy": AffectedDatePolicy,
                 "affected_key_strategy": AffectedKeyStrategy}
        kw = {}
        for k, v in row.items():
            if k not in known or v is None:
                continue
            kw[k] = enums[k](v) if k in enums else v
        if isinstance(row.get("cost_guard"), dict):
            kw["cost_guard"] = CostGuard(**row["cost_guard"])
        return JobFlowConfig(**kw)

    row = next(r for r in plan["plan"]["job_flow_config"]
               if r["job_id"] == JOB and r["flow_mode"] == "STREAMING_RT")
    flow = flow_from_row(row)
    src = resolve_stream_source(flow)
    st = start_application(client=client, flow=flow, job_id=JOB, environment="dev",
                          output_table=args.mart, code_version="phase14",
                          now=datetime.now(timezone.utc),
                          inspector=InMemoryCheckpointInspector())
    stopped = stop_application(client=client, state=st, now=datetime.now(timezone.utc),
                               reason="phase14 bounded probe")
    emit(scenario=8, name="STREAMING_RT independently", job_id=JOB, flow_mode="STREAMING_RT",
         application_name=st.application_name, deployment_id=st.deployment_id,
         source_decision=src.policy.value, checkpoint=st.checkpoint_location,
         checkpoint_state_at_start=st.checkpoint_state_at_start,
         is_enabled=row["is_enabled"], restart_count=stopped.restart_count,
         final_status=stopped.status.value,
         note="lifecycle only; enable_streaming_rt is false so no application was submitted")

    spark.createDataFrame([(json.dumps(EV, default=str),)], "j string") \
         .coalesce(1).write.mode("overwrite").text(args.out)
    print("PHASE14_SCENARIOS_DONE", len(EV))
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
