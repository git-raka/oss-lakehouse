"""BRONZE: raw data as-is + ingest metadata, loaded incrementally (Auto Loader equivalent).

Spark Structured Streaming with an availableNow trigger: only new files are processed and the
position is kept in a checkpoint, so re-running is safe (idempotent).
"""
from pyspark.sql import functions as F

from lakehouse.common import LAKE_BUCKET, landing_path, spark, table_path, volume_path

CHECKPOINTS = f"s3a://{LAKE_BUCKET}/_checkpoints/bronze"

# bronze table -> (landing folder, format, options)
SOURCES = {
    **{f"erp_{t}": (f"erp/{t}", "json", {}) for t in
       ["tenant", "plot", "asset", "vendor", "purchase_order", "employee"]},
    "weighbridge": ("weighbridge", "csv", {"header": "true"}),  # all columns kept as raw strings
    "ticketing": ("ticketing", "json", {}),
    "iot_bus": ("iot_bus", "json", {}),
}
# unstructured files: bronze keeps a file catalog (path, size, mtime); the bytes stay in the Volume
BINARY = {
    "document_files": ("raw/documents", "*.pdf"),
    "photo_files": ("raw/photos", "*.jpg"),
}


def _has_files(path: str) -> bool:
    ss = spark()
    jvm = ss._jvm
    p = jvm.org.apache.hadoop.fs.Path(path)
    fs = p.getFileSystem(ss._jsc.hadoopConfiguration())
    return fs.exists(p) and len(fs.listStatus(p)) > 0


def _run_stream(df, table: str) -> int:
    target = table_path("bronze", table)
    q = (df.withColumn("_ingest_ts", F.current_timestamp())
           .writeStream.format("delta")
           .option("checkpointLocation", f"{CHECKPOINTS}/{table}")
           .option("mergeSchema", "true")
           .trigger(availableNow=True)
           .start(target))
    q.awaitTermination()
    return sum(p.get("numInputRows", 0) for p in q.recentProgress)


def ingest_structured(table: str, folder: str, fmt: str, opts: dict) -> int:
    ss = spark()
    src = landing_path(folder)
    if not _has_files(src):
        return 0
    schema = ss.read.format(fmt).options(**opts).load(src).schema   # schema inferred from existing files
    df = (ss.readStream.format(fmt).options(**opts).schema(schema).load(src)
            .withColumn("_source_file", F.col("_metadata.file_path")))
    return _run_stream(df, table)


def ingest_binary(table: str, folder: str, glob: str) -> int:
    ss = spark()
    src = volume_path(folder)
    if not _has_files(src):
        return 0
    df = (ss.readStream.format("binaryFile").option("pathGlobFilter", glob)
            .schema("path string, modificationTime timestamp, length long, content binary")
            .load(src)
            .select("path", "modificationTime", "length"))   # file bytes are not duplicated into the table
    return _run_stream(df, table)


def run() -> dict:
    stats = {}
    for table, (folder, fmt, opts) in SOURCES.items():
        stats[table] = ingest_structured(table, folder, fmt, opts)
    for table, (folder, glob) in BINARY.items():
        stats[table] = ingest_binary(table, folder, glob)
    return stats


if __name__ == "__main__":
    print(run())
