import sys, os
_here = os.path.dirname(__file__)
for _p in ("jobs/l1_stream", "eod", "snapshot", "common", "dimensions", "facts", "marts",
           "reporting"):
    sys.path.insert(0, os.path.join(_here, "..", _p))
# The reporting tests import the compiler CLI and read reporting/ fixtures, so the repo
# root has to be importable too.
sys.path.insert(0, os.path.join(_here, "..", ".."))


# --------------------------------------------------------------------------- #
# ONE SparkSession for the whole suite.
#
# Ten modules each defined their own `spark` fixture ending in `getOrCreate()`. That is
# not ten sessions: there is one JVM per pytest process, so the FIRST session built wins
# and every later builder's configs are silently discarded. Collection is alphabetical, so
# the winner was test_ai_features' session -- which configures no Iceberg at all. Every
# module after it that needed `spark.sql.catalog.local` failed at fixture setup with
# `catalogPluginClassNotFoundForCatalogError`: 61 errors from a suite whose files all pass
# individually. Each module's `s.stop()` then tore the shared context down under the ones
# still to run.
#
# So the session is built ONCE here, with the union of what the modules asked for, and the
# per-module fixtures are gone. Namespaces are already per-test (`local.f<hash>`), so one
# warehouse is enough to keep them apart.
# --------------------------------------------------------------------------- #

import pytest

ICEBERG_PKG = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from pyspark.sql import SparkSession
    wh = str(tmp_path_factory.mktemp("warehouse"))
    try:
        s = (SparkSession.builder.appName("cdc-lakehouse-tests").master("local[2]")
             .config("spark.jars.packages", ICEBERG_PKG)
             .config("spark.sql.extensions",
                     "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.local.type", "hadoop")
             .config("spark.sql.catalog.local.warehouse", wh)
             .config("spark.sql.shuffle.partitions", "2")
             # ADR-024. Not a test convenience: several suites assert on dates derived in
             # the session zone, and they must assert against the zone production pins.
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.enabled", "false").getOrCreate())
    except Exception as exc:                      # offline / no jars
        pytest.skip(f"local Spark+Iceberg unavailable: {exc}")
    yield s
    s.stop()
