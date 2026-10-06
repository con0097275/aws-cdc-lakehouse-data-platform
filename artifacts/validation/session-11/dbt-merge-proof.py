"""Does dbt's incremental_predicates approach preserve the anti-downgrade rule?"""
import tempfile
from pyspark.sql import SparkSession
wh = tempfile.mkdtemp()
s = (SparkSession.builder.appName("merge-proof").master("local[2]")
     .config("spark.jars.packages","org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2")
     .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
     .config("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog")
     .config("spark.sql.catalog.local.type","hadoop").config("spark.sql.catalog.local.warehouse",wh)
     .config("spark.sql.session.timeZone","UTC").config("spark.ui.enabled","false").getOrCreate())
s.sparkContext.setLogLevel("ERROR")
s.sql("CREATE NAMESPACE IF NOT EXISTS local.p")
s.sql("""CREATE TABLE local.p.mart (transaction_id STRING, amount DECIMAL(18,2),
         processing_status STRING) USING iceberg""")
s.sql("INSERT INTO local.p.mart VALUES ('T1', 100.00, 'CERTIFIED')")
s.sql("""CREATE OR REPLACE TEMP VIEW src AS
         SELECT 'T1' AS transaction_id, CAST(999.00 AS DECIMAL(18,2)) AS amount,
                'PROVISIONAL_NRT' AS processing_status""")

RANK = "(CASE {c} WHEN 'PROVISIONAL_NRT' THEN 1 WHEN 'PROVISIONAL_CORRECTED' THEN 2 WHEN 'RECONCILED' THEN 3 WHEN 'CERTIFIED' THEN 4 ELSE 0 END)"
src_rank = RANK.format(c="DBT_INTERNAL_SOURCE.processing_status")
dst_rank = RANK.format(c="DBT_INTERNAL_DEST.processing_status")

# This is the shape dbt-spark generates: predicates ALL go in the ON clause,
# and the MATCHED clause is unconditional.
s.sql(f"""
  merge into local.p.mart as DBT_INTERNAL_DEST
    using src as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.transaction_id = DBT_INTERNAL_DEST.transaction_id
       and {src_rank} >= {dst_rank}
    when matched then update set *
    when not matched then insert *
""")
rows = s.sql("SELECT * FROM local.p.mart ORDER BY processing_status").collect()
print("ROWCOUNT:", len(rows))
for r in rows: print("  ", r)
print("VERDICT:", "DUPLICATE FACT CREATED" if len(rows) > 1 else "ok")
s.stop()
