"""Dagster orchestration (Lakeflow Jobs equivalent): asset lineage, schedules, data-quality checks, maintenance."""
import os

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    MaterializeResult,
    ScheduleDefinition,
    asset,
    asset_check,
    define_asset_job,
    in_process_executor,
    job,
    op,
)


def _flat(stats: dict) -> dict:
    return {k: (v if isinstance(v, (int, float, str)) else str(v)) for k, v in stats.items()}


# ------------------------------------------------------------------ pipeline assets
@asset(group_name="ingestion", kinds={"python", "postgres", "s3"},
       description="Edge server: ERP CDC (watermark) + daily files -> landing zone; PDFs/photos -> Volume")
def landing_zone(context: AssetExecutionContext):
    from lakehouse import edge
    stats = edge.run()
    context.log.info(stats)
    return MaterializeResult(metadata=_flat(stats))


@asset(group_name="bronze", deps=[landing_zone], kinds={"spark", "delta"},
       description="Incremental landing -> Bronze Delta (Structured Streaming availableNow + checkpoint)")
def bronze_tables(context: AssetExecutionContext):
    from lakehouse import bronze
    stats = bronze.run()
    context.log.info(stats)
    return MaterializeResult(metadata=_flat(stats))


@asset(group_name="silver", deps=[bronze_tables], kinds={"spark", "delta"},
       description="Cleansing, de-duplication and MERGE (CDC upsert) of structured data")
def silver_structured(context: AssetExecutionContext):
    from lakehouse import silver
    stats = silver.structured()
    context.log.info(stats)
    return MaterializeResult(metadata=_flat(stats))


@asset(group_name="silver", deps=[bronze_tables], kinds={"spark", "delta", "tesseract"},
       description="PDF -> text (text layer / Tesseract OCR) + extracted number, tenant, plot, value, date")
def silver_documents(context: AssetExecutionContext):
    from lakehouse import silver
    stats = silver.documents()
    context.log.info(stats)
    return MaterializeResult(metadata=_flat(stats))


@asset(group_name="silver", deps=[bronze_tables], kinds={"spark", "delta"},
       description="Photos -> EXIF (timestamp, GPS, camera, asset code) + thumbnail in a Volume")
def silver_photos(context: AssetExecutionContext):
    from lakehouse import silver
    stats = silver.photos()
    context.log.info(stats)
    return MaterializeResult(metadata=_flat(stats))


@asset(group_name="gold", deps=[silver_structured, silver_documents, silver_photos], kinds={"spark", "delta"},
       description="Consumption tables: tenant 360, estate summary, procurement, e-bus, weighbridge, documents, photos")
def gold_tables(context: AssetExecutionContext):
    from lakehouse import gold
    stats = gold.run()
    context.log.info(stats)
    return MaterializeResult(metadata=_flat(stats))


@asset(group_name="serving", deps=[gold_tables], kinds={"trino"},
       description="Register tables in the Trino catalog (SQL + row filters / column masks)")
def trino_catalog(context: AssetExecutionContext):
    from lakehouse import publish
    stats = publish.run()
    context.log.info(stats)
    return MaterializeResult(metadata={k: str(v) for k, v in stats.items()})


# ------------------------------------------------------------------ data-quality checks
def _q(sql):
    from lakehouse import publish
    from lakehouse.common import trino

    cur = trino("pipeline").cursor()
    try:
        cur.execute(sql)
        return cur.fetchall()
    except Exception as e:  # first install: table not registered in the catalog yet -> register, retry
        if "TABLE_NOT_FOUND" not in str(e) and "SCHEMA_NOT_FOUND" not in str(e):
            raise
        publish.run()
        cur.execute(sql)
        return cur.fetchall()


@asset_check(asset=silver_structured, description="Share of invalid weighbridge records (tare >= gross) < 5%")
def weighbridge_valid_ratio():
    total, invalid = _q("SELECT count(*), count_if(NOT is_valid) FROM delta.silver.weighbridge")[0]
    ratio = invalid / total if total else 0
    return AssetCheckResult(passed=ratio < 0.05, metadata={"total": total, "invalid": invalid,
                                                          "ratio": round(ratio, 4)})


@asset_check(asset=silver_structured, description="E-bus telemetry is fresh (last signal < 2 hours ago)")
def bus_freshness():
    lag = _q("SELECT date_diff('minute', max(ts), current_timestamp) FROM delta.silver.bus_telemetry")[0][0]
    return AssetCheckResult(passed=lag is not None and lag < 120, metadata={"lag_minutes": lag})


@asset_check(asset=silver_documents, description="Documents with missing key fields -> manual review queue")
def document_review_queue():
    n = _q("SELECT count_if(needs_review) FROM delta.silver.document")[0][0]
    return AssetCheckResult(passed=n == 0, severity=AssetCheckSeverity.WARN, metadata={"needs_review": n})


@asset_check(asset=gold_tables, description="Every active tenant has a digitised lease agreement")
def tenant_has_contract():
    n = _q("SELECT count(*) FROM delta.gold.tenant_360 WHERE status = 'ACTIVE' AND contract_number IS NULL")[0][0]
    return AssetCheckResult(passed=n == 0, severity=AssetCheckSeverity.WARN, metadata={"without_contract": n})


# ------------------------------------------------------------------ maintenance (OPTIMIZE + VACUUM)
@op
def optimize_vacuum(context):
    from delta.tables import DeltaTable

    from lakehouse.common import spark, table_path
    from lakehouse.publish import delta_tables

    ss = spark()
    for layer in ("bronze", "silver", "gold"):
        for t in delta_tables(layer):
            tbl = DeltaTable.forPath(ss, table_path(layer, t))
            tbl.optimize().executeCompaction()
            tbl.vacuum(168)  # keep 7 days of history for time travel
            context.log.info(f"OPTIMIZE+VACUUM {layer}.{t}")


@job(executor_def=in_process_executor, description="Compact small files & remove files older than 7 days")
def lakehouse_maintenance():
    optimize_vacuum()


pipeline_job = define_asset_job("lakehouse_pipeline", selection=AssetSelection.all(),
                                executor_def=in_process_executor,
                                description="Ingest -> Bronze -> Silver -> Gold -> Trino")

defs = Definitions(
    assets=[landing_zone, bronze_tables, silver_structured, silver_documents, silver_photos, gold_tables,
            trino_catalog],
    asset_checks=[weighbridge_valid_ratio, bus_freshness, document_review_queue, tenant_has_contract],
    jobs=[pipeline_job, lakehouse_maintenance],
    schedules=[
        ScheduleDefinition(job=pipeline_job, cron_schedule="*/30 * * * *", execution_timezone=os.getenv("TZ", "UTC"),
                           default_status=DefaultScheduleStatus.RUNNING),
        ScheduleDefinition(job=lakehouse_maintenance, cron_schedule="0 2 * * 0", execution_timezone=os.getenv("TZ", "UTC"),
                           default_status=DefaultScheduleStatus.RUNNING),
    ],
)
