"""GOLD: consumption-ready tables (executive & divisional dashboards, Data API, operations centre).

Every run writes a new Delta version (atomic overwrite) -> history is available for time travel.
"""
from lakehouse.common import spark, table_path

SILVER = ["tenant", "plot", "asset", "vendor", "purchase_order", "employee",
          "weighbridge", "ticket", "bus_telemetry", "document", "photo"]

GOLD_SQL = {
    "tenant_360": """
        WITH lease AS (
          SELECT tenant_id, max_by(doc_number, doc_date) AS contract_number,
                 max_by(amount_idr, doc_date) AS contract_value_idr, max(doc_date) AS contract_date
          FROM document WHERE doc_type = 'LEASE AGREEMENT' GROUP BY tenant_id),
        docs AS (SELECT tenant_id, count(*) AS document_count, count_if(needs_review) AS documents_needing_review
                 FROM document GROUP BY tenant_id),
        tk AS (SELECT tenant_id, count_if(status <> 'RESOLVED') AS open_tickets, count(*) AS total_tickets,
                      round(avg(CASE WHEN status = 'RESOLVED'
                        THEN (unix_timestamp(resolved_at) - unix_timestamp(created_at)) / 3600 END), 1)
                        AS avg_resolution_hours
               FROM ticket GROUP BY tenant_id),
        wb AS (SELECT tenant_id, count(*) AS trucks_30d, round(sum(net_kg) / 1000, 1) AS tonnage_30d
               FROM weighbridge WHERE is_valid AND time_in >= current_date() - INTERVAL 30 DAYS GROUP BY tenant_id)
        SELECT t.tenant_id, t.name AS tenant_name, t.sector, t.status, t.plot_id, p.zone,
               CAST(p.area_ha AS DOUBLE) AS area_ha, t.joined_date,
               lease.contract_number, lease.contract_value_idr, lease.contract_date,
               coalesce(docs.document_count, 0) AS document_count,
               coalesce(docs.documents_needing_review, 0) AS documents_needing_review,
               coalesce(tk.open_tickets, 0) AS open_tickets, coalesce(tk.total_tickets, 0) AS total_tickets,
               tk.avg_resolution_hours,
               coalesce(wb.trucks_30d, 0) AS trucks_30d, coalesce(wb.tonnage_30d, 0) AS tonnage_30d,
               t.pic_name, t.pic_phone, t.pic_email, t.pic_national_id,
               current_timestamp() AS refreshed_at
        FROM tenant t
        LEFT JOIN plot p USING (plot_id)
        LEFT JOIN lease USING (tenant_id)
        LEFT JOIN docs USING (tenant_id)
        LEFT JOIN tk USING (tenant_id)
        LEFT JOIN wb USING (tenant_id)
    """,
    "estate_summary": """
        SELECT
          (SELECT count(*) FROM tenant WHERE status = 'ACTIVE') AS active_tenants,
          (SELECT count(*) FROM plot) AS total_plots,
          (SELECT count(*) FROM plot WHERE status = 'OCCUPIED') AS occupied_plots,
          (SELECT round(100 * sum(CASE WHEN status = 'OCCUPIED' THEN area_ha END) / sum(area_ha), 1) FROM plot)
            AS land_occupancy_pct,
          (SELECT sum(amount_idr) FROM document WHERE doc_type = 'LEASE AGREEMENT') AS total_lease_value_idr,
          (SELECT sum(amount) FROM purchase_order WHERE year(po_date) = year(current_date())) AS po_value_ytd_idr,
          (SELECT count_if(status <> 'RESOLVED') FROM ticket) AS open_tickets,
          (SELECT round(sum(km), 0) FROM (SELECT bus_id, max(odometer_km) - min(odometer_km) AS km
             FROM bus_telemetry WHERE ts >= current_date() - INTERVAL 7 DAYS GROUP BY bus_id)) AS fleet_km_7d,
          (SELECT count_if(needs_review) FROM document) AS documents_needing_review,
          current_timestamp() AS refreshed_at
    """,
    "procurement_monthly": """
        SELECT CAST(date_trunc('MONTH', po_date) AS DATE) AS month, category,
               count(*) AS po_count, sum(amount) AS total_amount_idr,
               count_if(status = 'PAID') AS po_paid, count_if(status IN ('DRAFT', 'APPROVED')) AS po_in_progress,
               count(DISTINCT vendor_id) AS vendor_count
        FROM purchase_order GROUP BY 1, 2
    """,
    "bus_daily": """
        SELECT date, bus_id,
               round(max(odometer_km) - min(odometer_km), 1) AS km_travelled,
               round(sum(energy_kwh), 1) AS energy_kwh,
               round(sum(energy_kwh) / nullif(max(odometer_km) - min(odometer_km), 0), 3) AS kwh_per_km,
               round(avg(soc_pct), 1) AS avg_soc_pct, min(soc_pct) AS min_soc_pct,
               sum(boarding) AS passengers,
               round(avg(CASE WHEN speed_kmh > 0 THEN speed_kmh END), 1) AS avg_speed_kmh,
               count(*) AS signal_count, max(ts) AS last_signal
        FROM bus_telemetry GROUP BY date, bus_id
    """,
    "weighbridge_daily": """
        SELECT CAST(time_in AS DATE) AS date, tenant_id, commodity,
               count_if(is_valid) AS trucks, round(sum(CASE WHEN is_valid THEN net_kg END) / 1000, 2) AS net_tonnes,
               count_if(NOT is_valid) AS invalid_records
        FROM weighbridge GROUP BY 1, 2, 3
    """,
    "tenant_tickets": """
        SELECT k.ticket_id, k.tenant_id, t.name AS tenant_name, k.category, k.priority, k.status, k.title,
               k.created_at, k.resolved_at,
               round((unix_timestamp(coalesce(k.resolved_at, current_timestamp()))
                      - unix_timestamp(k.created_at)) / 3600, 1) AS age_hours
        FROM ticket k LEFT JOIN tenant t USING (tenant_id)
    """,
    "document_catalog": """
        SELECT d.file_name, d.doc_type, d.doc_number, d.tenant_id, t.name AS tenant_name,
               d.plot_id, d.doc_date, d.amount_idr, d.extraction_method, d.page_count,
               d.needs_review, substr(regexp_replace(d.text, '\\\\s+', ' '), 1, 300) AS summary,
               d.text, replace(d.path, 's3a://', 's3://') AS file_location, d.processed_at
        FROM document d LEFT JOIN tenant t USING (tenant_id)
    """,
    "asset_photos": """
        SELECT f.file_name, f.asset_id, a.name AS asset_name, a.asset_type, a.condition,
               f.taken_at, f.lat, f.lon, f.camera, f.description, f.needs_review,
               f.thumbnail_key, replace(f.path, 's3a://', 's3://') AS file_location
        FROM photo f LEFT JOIN asset a USING (asset_id)
    """,
    "asset_condition": """
        SELECT a.asset_type, a.condition, count(*) AS asset_count, sum(a.acquisition_value) AS acquisition_value_idr,
               count(f.asset_id) AS inspection_photos, max(f.taken_at) AS last_inspection
        FROM asset a LEFT JOIN photo f USING (asset_id) GROUP BY 1, 2
    """,
}


def run() -> dict:
    ss = spark()
    for t in SILVER:
        ss.read.format("delta").load(table_path("silver", t)).createOrReplaceTempView(t)
    stats = {}
    for table, sql in GOLD_SQL.items():
        df = ss.sql(sql)
        (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
           .save(table_path("gold", table)))
        stats[table] = df.count()
    return stats


if __name__ == "__main__":
    print(run())
