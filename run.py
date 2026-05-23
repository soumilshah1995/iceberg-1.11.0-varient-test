"""
Iceberg Variant Shredding Performance Test Suite (single-table mode).

Run once per shredding setting via spark-submit, passing a distinct --tablename each time.
Shredding is controlled by spark-submit conf:
  --conf spark.sql.iceberg.shred-variants=false
  --conf spark.sql.iceberg.shred-variants=true
"""
import argparse
import csv
import os
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass, field
from typing import Dict, List, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, parse_json, to_json, struct, monotonically_increasing_id, expr


@dataclass
class TestResult:
    operation: str
    shredding_enabled: bool
    epoch: int
    duration: float
    row_count: int = 0
    result_value: object = None


@dataclass
class OperationSummary:
    operation: str
    times: List[float] = field(default_factory=list)

    @property
    def avg(self) -> float:
        return statistics.mean(self.times) if self.times else 0

    @property
    def median(self) -> float:
        return statistics.median(self.times) if self.times else 0

    @property
    def stdev(self) -> float:
        return statistics.stdev(self.times) if len(self.times) > 1 else 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark Iceberg variant shredding on a single table (EMR spark-submit)."
    )
    parser.add_argument(
        "--tablename",
        required=True,
        help="Table name (short name or fully qualified catalog.namespace.table)",
    )
    parser.add_argument(
        "--catalog",
        default="s3tablesbucket",
        help="Default Iceberg catalog when tablename is not fully qualified (default: s3tablesbucket)",
    )
    parser.add_argument(
        "--namespace",
        default="benchmark",
        help="Namespace when tablename is not fully qualified (default: benchmark)",
    )
    parser.add_argument(
        "--data-path",
        default="github_archive.json.gz",
        help="Path to input JSON/GZ data (default: github_archive.json.gz)",
    )
    parser.add_argument(
        "--num-epochs",
        type=int,
        default=3,
        help="Repeat count for read benchmarks (default: 3)",
    )
    parser.add_argument(
        "--num-appends",
        type=int,
        default=5,
        help="Sequential append count before read tests (default: 5)",
    )
    parser.add_argument(
        "--drop-if-exists",
        action="store_true",
        default=False,
        help="Drop and recreate table (needs s3tables:DeleteTable IAM; default: false)",
    )
    parser.add_argument(
        "--truncate-if-exists",
        action="store_true",
        default=True,
        help="If table exists, TRUNCATE data before appends (default: true)",
    )
    parser.add_argument(
        "--no-truncate-if-exists",
        action="store_false",
        dest="truncate_if_exists",
        help="Keep existing rows and append on top",
    )
    parser.add_argument(
        "--results-file",
        default=None,
        help="CSV output path (default: <tablename>_results.csv)",
    )
    return parser.parse_args()


def resolve_table_name(catalog: str, namespace: str, tablename: str) -> str:
    parts = tablename.split(".")
    if len(parts) == 3:
        cat, ns, table = parts
        return f"{cat}.{ns}.{table.lower()}"
    if len(parts) == 2:
        return f"{catalog}.{parts[0]}.{parts[1].lower()}"
    return f"{catalog}.{namespace}.{tablename.lower()}"


def shred_variants_enabled(spark: SparkSession) -> bool:
    value = spark.conf.get("spark.sql.iceberg.shred-variants", "false").lower()
    return value in ("true", "1", "yes")


def create_spark_session() -> SparkSession:
    print("Creating Spark session...")
    spark = SparkSession.builder.appName("IcebergVariantBenchmark").getOrCreate()
    shred = shred_variants_enabled(spark)
    catalog = spark.conf.get("spark.sql.defaultCatalog", "spark_catalog")
    print(f"✓ Spark session ready (defaultCatalog={catalog}, shred-variants={shred})")
    return spark


def resolve_data_path(spark: SparkSession, path: str) -> str:
    """Resolve input path for EMR/YARN.

    Bare paths like /home/hadoop/foo are treated as HDFS by Spark, not local disk.
    If the file exists on the driver filesystem, upload it to HDFS so executors can read it.
    """
    if "://" in path:
        return path

    local_path = os.path.abspath(path)
    if os.path.isfile(local_path):
        hdfs_dir = "/user/hadoop/benchmark-data"
        hdfs_path = f"{hdfs_dir}/{os.path.basename(local_path)}"
        print(f"Local file found on driver — copying to HDFS: {hdfs_path}")
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir], check=True)
        subprocess.run(["hdfs", "dfs", "-put", "-f", local_path, hdfs_path], check=True)
        print(f"✓ Data available at {hdfs_path}")
        return hdfs_path

    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(conf)
    hdfs_path = jvm.org.apache.hadoop.fs.Path(path)
    if fs.exists(hdfs_path):
        print(f"✓ Using existing HDFS path: {path}")
        return path

    raise FileNotFoundError(
        f"Data path not found locally or on HDFS: {path}. "
        f"Upload first: hdfs dfs -put {local_path} /user/hadoop/benchmark-data/"
    )


class DataManager:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._cached_df = None

    def load_github_data(self, path: str) -> DataFrame:
        print(f"Loading data from {path}...")
        df = self.spark.read.json(path)
        row_count = df.count()
        print(f"✓ Loaded {row_count:,} records")

        df_variant = df.select(
            monotonically_increasing_id().cast("bigint").alias("id"),
            parse_json(to_json(struct(*[col(c) for c in df.columns]))).alias("v"),
        )
        self._cached_df = df_variant
        print("✓ Converted to variant format")
        return df_variant

    def get_cached_data(self) -> DataFrame:
        if self._cached_df is None:
            raise ValueError("No cached data available. Call load_github_data() first.")
        return self._cached_df


class TableManager:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def ensure_namespace(self, table_name: str):
        parts = table_name.split(".")
        if len(parts) == 3:
            catalog, namespace, _ = parts
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")

    def _table_exists(self, table_name: str) -> bool:
        try:
            self.spark.table(table_name).schema
            return True
        except Exception:
            return False

    def prepare_table(
        self,
        table_name: str,
        shredding_enabled: bool,
        drop_if_exists: bool = False,
        truncate_if_exists: bool = True,
    ):
        """Create table if missing; never drop unless explicitly requested."""
        self.ensure_namespace(table_name)
        shred_prop = "true" if shredding_enabled else "false"

        if drop_if_exists:
            self.spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            print(f"✓ Dropped table {table_name} (requires s3tables:DeleteTable)")

        if not self._table_exists(table_name):
            tblprops = f"'format-version' = '3', 'write.parquet.shred-variants' = '{shred_prop}'"
            if shredding_enabled:
                tblprops += ", 'write.parquet.variant-inference-buffer-size' = '10000'"

            self.spark.sql(f"""
            CREATE TABLE {table_name} (
                id BIGINT,
                v VARIANT
            )
            USING iceberg
            TBLPROPERTIES (
                {tblprops}
            )
            """)
            print(f"✓ Created table {table_name} (write.parquet.shred-variants={shred_prop})")
            return

        print(f"✓ Using existing table {table_name}")
        if truncate_if_exists:
            before = self.spark.table(table_name).count()
            try:
                self.spark.sql(f"TRUNCATE TABLE {table_name}")
            except Exception as exc:
                print(f"⚠ TRUNCATE failed ({exc}); falling back to DELETE FROM")
                self.spark.sql(f"DELETE FROM {table_name}")
            after = self.spark.table(table_name).count()
            print(f"✓ Cleared {table_name} ({before:,} → {after:,} rows)")
        else:
            rows = self.spark.table(table_name).count()
            print(f"⚠ Appending to existing table ({rows:,} rows already present)")


class WriteOperations:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def append(self, df: DataFrame, table_name: str) -> float:
        start = time.time()
        df.writeTo(table_name).append()
        return time.time() - start


class TestRunner:
    def __init__(
        self,
        spark: SparkSession,
        table_name: str,
        shredding_enabled: bool,
        num_epochs: int = 3,
        num_appends: int = 5,
    ):
        self.spark = spark
        self.table_name = table_name
        self.shredding_enabled = shredding_enabled
        self.num_epochs = num_epochs
        self.num_appends = num_appends
        self.results: List[TestResult] = []
        self.summaries: Dict[str, OperationSummary] = {}

        self.data_manager = DataManager(spark)
        self.table_manager = TableManager(spark)
        self.write_ops = WriteOperations(spark)

    def setup(
        self,
        data_path: str,
        drop_if_exists: bool = False,
        truncate_if_exists: bool = True,
    ):
        print("\n" + "=" * 80)
        print("ICEBERG VARIANT SHREDDING BENCHMARK (single table)")
        print("=" * 80)
        print(f"  Table:          {self.table_name}")
        print(f"  Shred variants: {self.shredding_enabled}")
        print(f"  Data path:      {data_path}")
        print(f"  Appends:        {self.num_appends}")
        print(f"  Read epochs:    {self.num_epochs}")
        print(f"  Drop table:     {drop_if_exists}")
        print(f"  Truncate data:  {truncate_if_exists}")

        resolved_path = resolve_data_path(self.spark, data_path)
        self.data_manager.load_github_data(resolved_path)
        self.table_manager.prepare_table(
            self.table_name,
            shredding_enabled=self.shredding_enabled,
            drop_if_exists=drop_if_exists,
            truncate_if_exists=truncate_if_exists,
        )
        print()

    def _record(self, operation: str, epoch: int, duration: float, result_value=None):
        self.results.append(
            TestResult(
                operation=operation,
                shredding_enabled=self.shredding_enabled,
                epoch=epoch,
                duration=duration,
                result_value=result_value,
            )
        )
        summary = self.summaries.setdefault(operation, OperationSummary(operation=operation))
        summary.times.append(duration)

    def _format_result(self, result) -> str:
        if result is None:
            return "NULL"
        if isinstance(result, float):
            return f"{result:,.2f}"
        if isinstance(result, int):
            return f"{result:,}"
        return str(result)

    def _run_timed_query(self, operation: str, query: str, epoch: int):
        start = time.time()
        result = self.spark.sql(query).collect()[0][0]
        duration = time.time() - start
        self._record(operation, epoch, duration, result)
        print(f"    ✓ {operation}: {duration:.2f}s (result: {self._format_result(result)})")
        return duration, result

    def _run_timed_group_query(self, operation: str, query: str, epoch: int):
        start = time.time()
        groups = self.spark.sql(query).count()
        duration = time.time() - start
        self._record(operation, epoch, duration, groups)
        print(f"    ✓ {operation}: {duration:.2f}s (groups: {groups})")
        return duration, groups

    def _run_read_benchmark(self, operation: str, title: str, query: str, *, group_by: bool = False):
        """Run the same read query num_epochs times (after all appends complete)."""
        print(f"\n{'=' * 80}")
        print(title)
        print(f"{'=' * 80}")
        for epoch in range(1, self.num_epochs + 1):
            print(f"\nEpoch {epoch}/{self.num_epochs}:")
            if group_by:
                self._run_timed_group_query(operation, query, epoch)
            else:
                self._run_timed_query(operation, query, epoch)

    def test_sequential_appends(self):
        print(f"\n{'=' * 80}")
        print(f"SEQUENTIAL APPENDS ({self.num_appends} runs)")
        print(f"{'=' * 80}")

        df = self.data_manager.get_cached_data()
        for i in range(1, self.num_appends + 1):
            print(f"\nAppend {i}/{self.num_appends}:")
            start = time.time()
            self.write_ops.append(df, self.table_name)
            duration = time.time() - start
            self._record("WRITE_APPEND", i, duration)
            print(f"    ✓ WRITE_APPEND: {duration:.2f}s")

        row_count = self.spark.table(self.table_name).count()
        print(f"\nFinal row count: {row_count:,}")

    # Read benchmarks below use github_archive 2024-01-15-12 (~287k rows/load).
    # Selectivity notes are per single append; table has num_appends copies after phase 1.

    def test_filter_level1(self):
        # GH event ids are ~34B; >1e9 matches ~100% of rows (was <1000 → 0 rows)
        self._run_read_benchmark(
            "FILTER_LEVEL1",
            "FILTER_LEVEL1 - $.id numeric gt (100% selectivity on GH archive)",
            f"""
            SELECT COUNT(*)
            FROM {self.table_name}
            WHERE CAST(variant_get(v, '$.id') AS BIGINT) > 1000000000
            """,
        )

    def test_filter_level2(self):
        # PushEvent ~70% of hour-12 file
        self._run_read_benchmark(
            "FILTER_LEVEL2",
            "FILTER_LEVEL2 - $.type = PushEvent (~70%)",
            f"""
            SELECT COUNT(*)
            FROM {self.table_name}
            WHERE CAST(variant_get(v, '$.type') AS STRING) = '"PushEvent"'
            """,
        )

    def test_filter_level3(self):
        self._run_read_benchmark(
            "FILTER_LEVEL3",
            "FILTER_LEVEL3 - $.actor.login IS NOT NULL (~100%)",
            f"""
            SELECT COUNT(*)
            FROM {self.table_name}
            WHERE variant_get(v, '$.actor.login') IS NOT NULL
            """,
        )

    def test_filter_level4(self):
        self._run_read_benchmark(
            "FILTER_LEVEL4",
            "FILTER_LEVEL4 - $.created_at LIKE hour prefix (~100%)",
            f"""
            SELECT COUNT(*)
            FROM {self.table_name}
            WHERE CAST(variant_get(v, '$.created_at') AS STRING) LIKE '%2024-01-15T12:%'
            """,
        )

    def test_filter_level5(self):
        self._run_read_benchmark(
            "FILTER_LEVEL5",
            "FILTER_LEVEL5 - $.type IN (4 common types) (~79%)",
            f"""
            SELECT COUNT(*)
            FROM {self.table_name}
            WHERE CAST(variant_get(v, '$.type') AS STRING) IN (
                '"PushEvent"', '"WatchEvent"', '"IssuesEvent"', '"IssueCommentEvent"'
            )
            """,
        )

    def test_filter_level6(self):
        self._run_read_benchmark(
            "FILTER_LEVEL6",
            "FILTER_LEVEL6 - $.public = true (~100%)",
            f"""
            SELECT COUNT(*)
            FROM {self.table_name}
            WHERE CAST(variant_get(v, '$.public') AS BOOLEAN) = true
            """,
        )

    def test_filter_level7(self):
        self._run_read_benchmark(
            "FILTER_LEVEL7",
            "FILTER_LEVEL7 - $.payload.action IN opened/created/closed (~14%)",
            f"""
            SELECT COUNT(*)
            FROM {self.table_name}
            WHERE CAST(variant_get(v, '$.payload.action') AS STRING) IN (
                '"opened"', '"created"', '"closed"'
            )
            """,
        )

    def test_filter_level8(self):
        self._run_read_benchmark(
            "FILTER_LEVEL8",
            "FILTER_LEVEL8 - $.org.login IS NOT NULL (~22%)",
            f"""
            SELECT COUNT(*)
            FROM {self.table_name}
            WHERE variant_get(v, '$.org.login') IS NOT NULL
            """,
        )

    def test_filter_level9(self):
        self._run_read_benchmark(
            "FILTER_LEVEL9",
            "FILTER_LEVEL9 - $.payload.ref IS NOT NULL (~79%)",
            f"""
            SELECT COUNT(*)
            FROM {self.table_name}
            WHERE variant_get(v, '$.payload.ref') IS NOT NULL
            """,
        )

    def test_filter_level10(self):
        self._run_read_benchmark(
            "FILTER_LEVEL10",
            "FILTER_LEVEL10 - $.type PushEvent AND $.actor.login (~70%)",
            f"""
            SELECT COUNT(*)
            FROM {self.table_name}
            WHERE CAST(variant_get(v, '$.type') AS STRING) = '"PushEvent"'
              AND variant_get(v, '$.actor.login') IS NOT NULL
            """,
        )

    def test_filter_level11(self):
        self._run_read_benchmark(
            "FILTER_LEVEL11",
            "FILTER_LEVEL11 - $.repo.name LIKE contains linux/coreutils (~low selectivity)",
            f"""
            SELECT COUNT(*)
            FROM {self.table_name}
            WHERE CAST(variant_get(v, '$.repo.name') AS STRING) LIKE '%linux%'
               OR CAST(variant_get(v, '$.repo.name') AS STRING) LIKE '%coreutils%'
            """,
        )

    def test_filter_level12(self):
        self._run_read_benchmark(
            "FILTER_LEVEL12",
            "FILTER_LEVEL12 - $.payload.commits IS NOT NULL (~70%, PushEvent branch)",
            f"""
            SELECT COUNT(*)
            FROM {self.table_name}
            WHERE variant_get(v, '$.payload.commits') IS NOT NULL
            """,
        )

    def test_agg_level1(self):
        self._run_read_benchmark(
            "AGG_LEVEL1",
            "AGG_LEVEL1 - SUM($.id)",
            f"""
            SELECT SUM(CAST(variant_get(v, '$.id') AS BIGINT)) as total
            FROM {self.table_name}
            """,
        )

    def test_agg_level2(self):
        self._run_read_benchmark(
            "AGG_LEVEL2",
            "AGG_LEVEL2 - GROUP BY $.type (top 10)",
            f"""
            SELECT
                CAST(variant_get(v, '$.type') AS STRING) as event_type,
                COUNT(*) as count
            FROM {self.table_name}
            GROUP BY CAST(variant_get(v, '$.type') AS STRING)
            ORDER BY count DESC
            LIMIT 10
            """,
            group_by=True,
        )

    def test_agg_level3(self):
        self._run_read_benchmark(
            "AGG_LEVEL3",
            "AGG_LEVEL3 - GROUP BY $.repo.name (top 10)",
            f"""
            SELECT
                CAST(variant_get(v, '$.repo.name') AS STRING) as repo,
                COUNT(*) as event_count
            FROM {self.table_name}
            WHERE variant_get(v, '$.repo.name') IS NOT NULL
            GROUP BY CAST(variant_get(v, '$.repo.name') AS STRING)
            ORDER BY event_count DESC
            LIMIT 10
            """,
            group_by=True,
        )

    def test_agg_level4(self):
        self._run_read_benchmark(
            "AGG_LEVEL4",
            "AGG_LEVEL4 - GROUP BY $.payload.action (deep nested, ~14% rows)",
            f"""
            SELECT
                CAST(variant_get(v, '$.payload.action') AS STRING) as action,
                COUNT(*) as count
            FROM {self.table_name}
            WHERE variant_get(v, '$.payload.action') IS NOT NULL
            GROUP BY CAST(variant_get(v, '$.payload.action') AS STRING)
            ORDER BY count DESC
            LIMIT 20
            """,
            group_by=True,
        )

    def test_agg_level5(self):
        self._run_read_benchmark(
            "AGG_LEVEL5",
            "AGG_LEVEL5 - GROUP BY $.org.login (~22% rows)",
            f"""
            SELECT
                CAST(variant_get(v, '$.org.login') AS STRING) as org,
                COUNT(*) as count
            FROM {self.table_name}
            WHERE variant_get(v, '$.org.login') IS NOT NULL
            GROUP BY CAST(variant_get(v, '$.org.login') AS STRING)
            ORDER BY count DESC
            LIMIT 20
            """,
            group_by=True,
        )

    def test_agg_level6(self):
        self._run_read_benchmark(
            "AGG_LEVEL6",
            "AGG_LEVEL6 - MIN/MAX($.id)",
            f"""
            SELECT
                MIN(CAST(variant_get(v, '$.id') AS BIGINT)) as min_id,
                MAX(CAST(variant_get(v, '$.id') AS BIGINT)) as max_id
            FROM {self.table_name}
            """,
        )

    def test_agg_level7(self):
        self._run_read_benchmark(
            "AGG_LEVEL7",
            "AGG_LEVEL7 - COUNT DISTINCT $.type",
            f"""
            SELECT COUNT(DISTINCT CAST(variant_get(v, '$.type') AS STRING)) as distinct_types
            FROM {self.table_name}
            """,
        )

    def test_agg_level8(self):
        # payload.size exists on PushEvent (~200k/load); AVG skips null casts
        self._run_read_benchmark(
            "AGG_LEVEL8",
            "AGG_LEVEL8 - AVG($.payload.size) on PushEvent subset",
            f"""
            SELECT AVG(CAST(variant_get(v, '$.payload.size') AS DOUBLE)) as avg_size
            FROM {self.table_name}
            WHERE CAST(variant_get(v, '$.type') AS STRING) = '"PushEvent"'
            """,
        )

    def test_agg_complex(self):
        self._run_read_benchmark(
            "AGG_COMPLEX",
            "AGG_COMPLEX - filter + GROUP BY $.type + COUNT DISTINCT $.actor.id",
            f"""
            SELECT
                CAST(variant_get(v, '$.type') AS STRING) as event_type,
                COUNT(*) as count,
                COUNT(DISTINCT CAST(variant_get(v, '$.actor.id') AS STRING)) as unique_actors
            FROM {self.table_name}
            WHERE CAST(variant_get(v, '$.type') AS STRING) IN (
                '"PushEvent"', '"WatchEvent"', '"PullRequestEvent"'
            )
            GROUP BY CAST(variant_get(v, '$.type') AS STRING)
            ORDER BY count DESC
            """,
            group_by=True,
        )

    def run_all_tests(self):
        self.test_sequential_appends()
        self.test_filter_level1()
        self.test_filter_level2()
        self.test_filter_level3()
        self.test_filter_level4()
        self.test_filter_level5()
        self.test_filter_level6()
        self.test_filter_level7()
        self.test_filter_level8()
        self.test_filter_level9()
        self.test_filter_level10()
        self.test_filter_level11()
        self.test_filter_level12()
        self.test_agg_level1()
        self.test_agg_level2()
        self.test_agg_level3()
        self.test_agg_level4()
        self.test_agg_level5()
        self.test_agg_level6()
        self.test_agg_level7()
        self.test_agg_level8()
        self.test_agg_complex()

    def print_summary_report(self):
        print("\n\n")
        print("=" * 100)
        print("BENCHMARK SUMMARY")
        print("=" * 100)
        print(f"  Table:          {self.table_name}")
        print(f"  Shred variants: {self.shredding_enabled}")
        print(f"  Read epochs:    {self.num_epochs}")
        print(f"  Append runs:    {self.num_appends}")

        print(f"\n{'=' * 100}")
        print(f"{'OPERATION':<20} {'AVG (s)':<12} {'MEDIAN (s)':<12} {'MIN (s)':<12} {'MAX (s)':<12}")
        print(f"{'=' * 100}")
        for op_name, summary in self.summaries.items():
            print(
                f"{op_name:<20} {summary.avg:>10.2f}s  {summary.median:>10.2f}s  "
                f"{min(summary.times):>10.2f}s  {max(summary.times):>10.2f}s"
            )
        print(f"{'=' * 100}\n")

    def export_results_csv(self, filename: str):
        with open(filename, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                ["Table", "Shredding", "Operation", "Epoch", "Duration (s)", "Result Value"]
            )
            for result in self.results:
                writer.writerow(
                    [
                        self.table_name,
                        "Yes" if result.shredding_enabled else "No",
                        result.operation,
                        result.epoch,
                        f"{result.duration:.2f}",
                        result.result_value,
                    ]
                )
        print(f"✓ Results exported to {filename}")


def main():
    args = parse_args()
    table_name = resolve_table_name(args.catalog, args.namespace, args.tablename)
    results_file = args.results_file or f"{args.tablename.replace('.', '_')}_results.csv"

    spark = create_spark_session()
    shredding_enabled = shred_variants_enabled(spark)

    try:
        runner = TestRunner(
            spark=spark,
            table_name=table_name,
            shredding_enabled=shredding_enabled,
            num_epochs=args.num_epochs,
            num_appends=args.num_appends,
        )
        runner.setup(
            data_path=args.data_path,
            drop_if_exists=args.drop_if_exists,
            truncate_if_exists=args.truncate_if_exists,
        )
        runner.run_all_tests()
        runner.print_summary_report()
        runner.export_results_csv(results_file)
    finally:
        spark.stop()
        print("Spark session stopped")


if __name__ == "__main__":
    main()
