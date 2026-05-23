

Setup
master : m7g.xlarge
core r6g.2xlarge X 3

# --- Download one hour of GitHub archive data (~117 MB) ---
cd /home/hadoop/soumil
curl -L -o github_archive.json.gz \
  "https://data.gharchive.org/2024-01-15-12.json.gz"

hdfs dfs -mkdir -p /user/hadoop/benchmark-data
hdfs dfs -put -f /home/hadoop/soumil/github_archive.json.gz /user/hadoop/benchmark-data/

export PACKAGES="com.amazonaws:aws-java-sdk-bundle:1.12.661,org.apache.hadoop:hadoop-aws:3.3.4,software.amazon.awssdk:bundle:2.29.38,com.github.ben-manes.caffeine:caffeine:3.1.8,org.apache.commons:commons-configuration2:2.11.0,org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0"
export SPARK_MASTER=yarn
export SPARK_DEPLOY_MODE=client
export NUM_EXECUTORS=4
export EXECUTOR_CORES=4
export EXECUTOR_MEMORY=8g
export DRIVER_MEMORY=4g
export DRIVER_CORES=2
export EXECUTOR_MEMORY_OVERHEAD=2g

SCRIPT=/home/hadoop/soumil/run.py
DATA=/user/hadoop/benchmark-data/github_archive.json.gz

# S3 Tables requires lowercase table names (gh_noshred ok, Gh_shred fails)

# --- Run 1: NO shredding ---
spark-submit \
  --master ${SPARK_MASTER} \
  --deploy-mode ${SPARK_DEPLOY_MODE} \
  --num-executors ${NUM_EXECUTORS} \
  --executor-cores ${EXECUTOR_CORES} \
  --executor-memory ${EXECUTOR_MEMORY} \
  --driver-memory ${DRIVER_MEMORY} \
  --driver-cores ${DRIVER_CORES} \
  --conf spark.executor.memoryOverhead=${EXECUTOR_MEMORY_OVERHEAD} \
  --conf spark.sql.shuffle.partitions=32 \
  --packages "${PACKAGES}" \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.s3tablesbucket.type=rest \
  --conf spark.sql.catalog.s3tablesbucket.uri=https://s3tables.us-east-1.amazonaws.com/iceberg \
  --conf spark.sql.catalog.s3tablesbucket.warehouse=XXXX \
  --conf spark.sql.catalog.s3tablesbucket.rest.auth.type=sigv4 \
  --conf spark.sql.catalog.s3tablesbucket.rest.signing-name=s3tables \
  --conf spark.sql.catalog.s3tablesbucket.rest.signing-region=us-east-1 \
  --conf spark.sql.catalog.s3tablesbucket.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.s3tablesbucket.table-default.write.parquet.shred-variants=false \
  --conf spark.sql.iceberg.shred-variants=false \
  --conf spark.sql.defaultCatalog=s3tablesbucket \
  "${SCRIPT}" \
  --tablename gh_noshred \
  --namespace test \
  --data-path "${DATA}" \
  --num-appends 5 \
  --num-epochs 5 \
  --results-file /tmp/gh_noshred_results.csv


# --- Run 2: WITH shredding ---
spark-submit \
  --master ${SPARK_MASTER} \
  --deploy-mode ${SPARK_DEPLOY_MODE} \
  --num-executors ${NUM_EXECUTORS} \
  --executor-cores ${EXECUTOR_CORES} \
  --executor-memory ${EXECUTOR_MEMORY} \
  --driver-memory ${DRIVER_MEMORY} \
  --driver-cores ${DRIVER_CORES} \
  --conf spark.executor.memoryOverhead=${EXECUTOR_MEMORY_OVERHEAD} \
  --conf spark.sql.shuffle.partitions=32 \
  --packages "${PACKAGES}" \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.s3tablesbucket.type=rest \
  --conf spark.sql.catalog.s3tablesbucket.uri=https://s3tables.us-east-1.amazonaws.com/iceberg \
  --conf spark.sql.catalog.s3tablesbucket.warehouse=XXX \
  --conf spark.sql.catalog.s3tablesbucket.rest.auth.type=sigv4 \
  --conf spark.sql.catalog.s3tablesbucket.rest.signing-name=s3tables \
  --conf spark.sql.catalog.s3tablesbucket.rest.signing-region=us-east-1 \
  --conf spark.sql.catalog.s3tablesbucket.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.s3tablesbucket.table-default.write.parquet.shred-variants=true \
  --conf spark.sql.iceberg.shred-variants=true \
  --conf spark.sql.defaultCatalog=s3tablesbucket \
  "${SCRIPT}" \
  --tablename gh_shred \
  --namespace test \
  --data-path "${DATA}" \
  --num-appends 5 \
  --num-epochs 5 \
  --results-file /tmp/gh_shred_results.csv

