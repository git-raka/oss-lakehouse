"""Shared configuration & connections: Spark + Delta, S3 (SeaweedFS), Trino, source Postgres."""
import functools
import os

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://seaweedfs:8333")
S3_ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY = os.environ["S3_SECRET_KEY"]
PG_DSN = os.environ["PG_DSN"]
SOURCES_DIR = os.getenv("SOURCES_DIR", "/sources")

LAKE_BUCKET = "lakehouse"     # bronze/silver/gold (Delta) + volumes (raw PDF/photo files)
LANDING_BUCKET = "landing"    # landing zone written by the edge server

LAYERS = ("bronze", "silver", "gold")


def table_path(layer: str, table: str, scheme: str = "s3a") -> str:
    return f"{scheme}://{LAKE_BUCKET}/{layer}/{table}"


def volume_path(name: str, scheme: str = "s3a") -> str:
    return f"{scheme}://{LAKE_BUCKET}/volumes/{name}"


def landing_path(source: str) -> str:
    return f"s3a://{LANDING_BUCKET}/{source}"


def s3():
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 5}),
    )


def ensure_buckets():
    client = s3()
    existing = {b["Name"] for b in client.list_buckets().get("Buckets", [])}
    for bucket in (LAKE_BUCKET, LANDING_BUCKET):
        if bucket not in existing:
            client.create_bucket(Bucket=bucket)


@functools.lru_cache(maxsize=1)
def spark(app: str = "oss-lakehouse"):
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app)
        .master(os.getenv("SPARK_MASTER", "local[4]"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", os.getenv("TZ", "UTC"))
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    return session


def trino(user: str = "pipeline"):
    import trino as trino_client

    return trino_client.dbapi.connect(
        host=os.getenv("TRINO_HOST", "trino"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        user=user,
        catalog="delta",
    )


def pg():
    import psycopg

    return psycopg.connect(PG_DSN, autocommit=True)
