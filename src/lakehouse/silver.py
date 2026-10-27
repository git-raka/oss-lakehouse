"""SILVER: clean, standardised, de-duplicated data; source changes are UPSERTed (MERGE) -> CDC support.

Includes unstructured-data processing:
- PDF    -> text (text layer via pdfplumber, Tesseract OCR fallback for scans) + extracted fields
- Photos -> EXIF metadata (timestamp, GPS, camera, asset code) + thumbnail written to a Volume
"""
from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from lakehouse.common import LAKE_BUCKET, spark, table_path

MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August",
          "September", "October", "November", "December"]


def bronze(table):
    return spark().read.format("delta").load(table_path("bronze", table))


def latest(df, keys, order_col):
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc_nulls_last(), F.col("_ingest_ts").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def upsert(df, table, keys) -> dict:
    ss = spark()
    path = table_path("silver", table)
    df = df.withColumn("_silver_ts", F.current_timestamp())
    if not DeltaTable.isDeltaTable(ss, path):
        df.write.format("delta").save(path)
        return {"inserted": df.count(), "updated": 0}
    tbl = DeltaTable.forPath(ss, path)
    cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    target_cols = set(tbl.toDF().columns)
    new_cols = [c for c in df.columns if c not in target_cols]
    changed = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in df.columns
                          if c in target_cols and c not in keys and c != "_silver_ts")
    if new_cols:          # schema evolved -> every matched row must be rewritten with the new columns
        changed = None
    (tbl.alias("t").merge(df.alias("s"), cond)
        .whenMatchedUpdateAll(condition=changed)       # only rows that actually changed
        .whenNotMatchedInsertAll()
        .execute())
    m = tbl.history(1).select("operationMetrics").first()[0]
    return {"inserted": int(m.get("numTargetRowsInserted", 0)), "updated": int(m.get("numTargetRowsUpdated", 0))}


# ------------------------------------------------------------------ structured data
ERP = {  # table: (keys, date columns, timestamp columns)
    "tenant": (["tenant_id"], ["joined_date"], ["updated_at"]),
    "plot": (["plot_id"], [], ["updated_at"]),
    "asset": (["asset_id"], [], ["updated_at"]),
    "vendor": (["vendor_id"], [], ["updated_at"]),
    "purchase_order": (["po_id"], ["po_date"], ["updated_at"]),
    "employee": (["employee_id"], ["hire_date"], ["updated_at"]),
}


def erp_tables() -> dict:
    stats = {}
    for table, (keys, dates, stamps) in ERP.items():
        df = bronze(f"erp_{table}")
        for c in dates:
            df = df.withColumn(c, F.to_date(c))
        for c in stamps:
            df = df.withColumn(c, F.to_timestamp(c))
        df = latest(df, keys, "updated_at").drop("_ingest_ts", "_source_file")
        stats[table] = upsert(df, table, keys)
    return stats


def weighbridge() -> dict:
    df = (bronze("weighbridge")
          .select("ticket_no",
                  F.to_timestamp("time_in").alias("time_in"),
                  F.to_timestamp("time_out").alias("time_out"),
                  F.upper(F.trim("plate_no")).alias("plate_no"),
                  "tenant_id", "commodity",
                  F.col("gross_kg").try_cast("long").alias("gross_kg"),
                  F.col("tare_kg").try_cast("long").alias("tare_kg"),
                  "operator", "_ingest_ts")
          .withColumn("net_kg", F.col("gross_kg") - F.col("tare_kg"))
          .withColumn("is_valid", F.coalesce(F.col("net_kg") > 0, F.lit(False)))
          .withColumn("quality_note", F.when(~F.col("is_valid"), "tare >= gross or missing weight")))
    df = latest(df, ["ticket_no"], "_ingest_ts")      # re-sent tickets -> keep the latest version
    return upsert(df.drop("_ingest_ts"), "weighbridge", ["ticket_no"])


def tickets() -> dict:
    df = (bronze("ticketing")
          .withColumn("created_at", F.to_timestamp("created_at"))
          .withColumn("resolved_at", F.to_timestamp("resolved_at"))
          .withColumn("updated_at", F.to_timestamp("updated_at")))
    df = latest(df, ["ticket_id"], "updated_at").drop("_ingest_ts", "_source_file")
    return upsert(df, "ticket", ["ticket_id"])


def bus_telemetry() -> dict:
    df = (bronze("iot_bus")
          .withColumn("ts", F.to_timestamp("ts"))
          .filter(F.col("soc_pct").between(0, 100) & F.col("bus_id").isNotNull())
          .withColumn("date", F.to_date("ts")))
    df = latest(df, ["bus_id", "ts"], "_ingest_ts").drop("_ingest_ts", "_source_file")
    return upsert(df, "bus_telemetry", ["bus_id", "ts"])


def structured() -> dict:
    stats = {f"erp.{k}": v for k, v in erp_tables().items()}
    stats["weighbridge"] = weighbridge()
    stats["ticket"] = tickets()
    stats["bus_telemetry"] = bus_telemetry()
    return stats


# ------------------------------------------------------------------ PDF -> text (+ OCR)
PDF_SCHEMA = T.StructType([
    T.StructField("text", T.StringType()),
    T.StructField("page_count", T.IntegerType()),
    T.StructField("extraction_method", T.StringType()),
])


@F.udf(PDF_SCHEMA)
def extract_pdf(content):
    import io

    import pdfplumber

    with pdfplumber.open(io.BytesIO(content)) as pdf:
        pages = len(pdf.pages)
        text = "\n".join((p.extract_text() or "") for p in pdf.pages)
    if len(text.strip()) >= 40:
        return (text, pages, "TEXT_LAYER")
    # no text layer -> scanned document -> OCR
    import pytesseract
    from pdf2image import convert_from_bytes

    images = convert_from_bytes(content, dpi=200)
    text = "\n".join(pytesseract.image_to_string(img, lang="eng+ind") for img in images)
    return (text, pages, "OCR_TESSERACT")


# Bump the version when extraction logic changes -> previously processed files are reprocessed
PARSER_VERSION = {"document": 1, "photo": 1}


def _new_files(bronze_table, silver_table):
    """Files not processed yet, or processed by an older parser version."""
    ss = spark()
    files = bronze(bronze_table).select("path", "modificationTime", "length").dropDuplicates(["path"])
    target = table_path("silver", silver_table)
    if DeltaTable.isDeltaTable(ss, target):
        done = ss.read.format("delta").load(target)
        if "parser_version" in done.columns:
            done = done.filter(F.col("parser_version") >= PARSER_VERSION[silver_table])
        else:
            done = done.limit(0)
        files = files.join(done.select("path"), "path", "left_anti")
    return [r.path for r in files.collect()]


def _ocr_code(col, pattern):
    """Tolerate common OCR misreads inside codes (O->0, I/l->1)."""
    return F.translate(F.regexp_extract(col, f"({pattern})", 1), "OoIl", "0011")


def documents() -> dict:
    paths = _new_files("document_files", "document")
    if not paths:
        return {"inserted": 0, "updated": 0, "new_files": 0}
    month_map = F.create_map(*[x for i, m in enumerate(MONTHS, 1) for x in (F.lit(m.lower()), F.lit(i))])
    long_date = r"(\d{1,2})\s+(" + "|".join(MONTHS) + r")\s+(\d{4})"
    df = (spark().read.format("binaryFile").load(paths)
          .select("path", "length", "modificationTime", extract_pdf("content").alias("x"))
          .select("path", "length", "modificationTime", "x.*")
          .withColumn("file_name", F.element_at(F.split("path", "/"), -1))
          .withColumn("doc_type",
                      F.when(F.col("text").rlike("(?i)LEASE AGREEMENT"), "LEASE AGREEMENT")
                       .when(F.col("text").rlike("(?i)HANDOVER CERTIFICATE"), "PLOT HANDOVER")
                       .when(F.col("text").rlike("(?i)BUILDING APPROVAL"), "BUILDING APPROVAL")
                       .otherwise("OTHER"))
          .withColumn("doc_number", F.regexp_extract("text", r"([A-Z]{3}/EST/\d{4}/\d{3})", 1))
          .withColumn("_code", F.regexp_extract("text", r"Tenant Code\s*:?\s*(\S+)", 1))
          .withColumn("tenant_id", _ocr_code("_code", "T[0-9OoIl]{3}"))
          .drop("_code")
          .withColumn("plot_id", _ocr_code("text", "P-[ABC][0-9OoIl]{2}"))
          .withColumn("amount_idr", F.regexp_replace(
              F.regexp_extract("text", r"(?:Contract|Investment) Value\s*:\s*IDR\s*([\d,]+)", 1), ",", "")
              .try_cast("long"))
          .withColumn("_d", F.regexp_extract("text", long_date, 1).try_cast("int"))
          .withColumn("doc_date", F.when(F.col("_d").between(1, 31), F.make_date(
              F.regexp_extract("text", long_date, 3).try_cast("int"),
              month_map[F.lower(F.regexp_extract("text", long_date, 2))], F.col("_d"))))
          .drop("_d")
          .withColumn("processed_at", F.current_timestamp()))
    for c in ("doc_number", "tenant_id", "plot_id"):
        df = df.withColumn(c, F.when(F.col(c) != "", F.col(c)))
    # data quality: missing key fields -> manual review queue
    df = (df.withColumn("needs_review", F.col("doc_number").isNull() | F.col("tenant_id").isNull()
                        | F.col("doc_date").isNull() | (F.col("doc_type") == "OTHER"))
            .withColumn("parser_version", F.lit(PARSER_VERSION["document"])))
    out = upsert(df, "document", ["path"])
    out["new_files"] = len(paths)
    return out


# ------------------------------------------------------------------ Photos -> EXIF + thumbnail
PHOTO_SCHEMA = T.StructType([
    T.StructField("taken_at", T.StringType()),
    T.StructField("lat", T.DoubleType()),
    T.StructField("lon", T.DoubleType()),
    T.StructField("camera", T.StringType()),
    T.StructField("description", T.StringType()),
    T.StructField("width_px", T.IntegerType()),
    T.StructField("height_px", T.IntegerType()),
    T.StructField("thumbnail_key", T.StringType()),
])


@F.udf(PHOTO_SCHEMA)
def extract_photo(path, content):
    import io

    from PIL import ExifTags, Image

    from lakehouse.common import s3

    img = Image.open(io.BytesIO(content))
    exif = img.getexif()
    base = {ExifTags.TAGS.get(k, k): v for k, v in exif.items()}
    sub = {ExifTags.TAGS.get(k, k): v for k, v in exif.get_ifd(ExifTags.IFD.Exif).items()}
    gps = {ExifTags.GPSTAGS.get(k, k): v for k, v in exif.get_ifd(ExifTags.IFD.GPSInfo).items()}

    def to_deg(v, ref):
        if not v:
            return None
        d = float(v[0]) + float(v[1]) / 60 + float(v[2]) / 3600
        return -d if ref in ("S", "W") else d

    taken = sub.get("DateTimeOriginal")
    taken = taken.replace(":", "-", 2) if taken else None
    name = path.rsplit("/", 1)[-1]
    thumb_key = f"volumes/thumbnails/{name}"
    thumb = img.copy()
    thumb.thumbnail((320, 320))
    buf = io.BytesIO()
    thumb.convert("RGB").save(buf, "JPEG", quality=80)
    s3().put_object(Bucket=LAKE_BUCKET, Key=thumb_key, Body=buf.getvalue(), ContentType="image/jpeg")
    return (taken, to_deg(gps.get("GPSLatitude"), gps.get("GPSLatitudeRef")),
            to_deg(gps.get("GPSLongitude"), gps.get("GPSLongitudeRef")),
            f"{base.get('Make', '')} {base.get('Model', '')}".strip() or None,
            base.get("ImageDescription"), img.width, img.height, thumb_key)


def photos() -> dict:
    paths = _new_files("photo_files", "photo")
    if not paths:
        return {"inserted": 0, "updated": 0, "new_files": 0}
    df = (spark().read.format("binaryFile").load(paths)
          .select("path", "length", extract_photo("path", "content").alias("x"))
          .select("path", "length", "x.*")
          .withColumn("file_name", F.element_at(F.split("path", "/"), -1))
          .withColumn("taken_at", F.to_timestamp("taken_at"))
          .withColumn("asset_id", F.regexp_extract("description", r"(AST-\d{4})", 1))
          .withColumn("asset_id", F.when(F.col("asset_id") != "", F.col("asset_id")))
          .withColumn("needs_review", F.col("asset_id").isNull() | F.col("lat").isNull())
          .withColumn("parser_version", F.lit(PARSER_VERSION["photo"]))
          .withColumn("processed_at", F.current_timestamp()))
    out = upsert(df, "photo", ["path"])
    out["new_files"] = len(paths)
    return out


if __name__ == "__main__":
    print(structured())
    print("documents", documents())
    print("photos", photos())
