"""Superset provisioning via REST API (idempotent): gold datasets + charts + executive dashboard.

    python -m lakehouse.superset_setup
"""
import json
import os
import time

import requests

BASE = os.getenv("SUPERSET_URL", "http://superset:8088")
DB_NAME = "Lakehouse"
TITLE = "Executive Dashboard"


class Superset:
    def __init__(self):
        self.s = requests.Session()
        for _ in range(30):
            try:
                r = self.s.post(f"{BASE}/api/v1/security/login", json={
                    "username": "admin", "password": os.environ["SUPERSET_ADMIN_PASSWORD"],
                    "provider": "db", "refresh": True})
                if r.ok:
                    break
            except requests.ConnectionError:
                pass
            time.sleep(5)
        r.raise_for_status()
        self.s.headers["Authorization"] = f"Bearer {r.json()['access_token']}"
        csrf = self.s.get(f"{BASE}/api/v1/security/csrf_token/").json()["result"]
        self.s.headers.update({"X-CSRFToken": csrf, "Referer": BASE})

    def call(self, method, path, **kw):
        r = self.s.request(method, f"{BASE}/api/v1/{path}", **kw)
        if not r.ok:
            raise RuntimeError(f"{method} {path}: {r.status_code} {r.text[:300]}")
        return r.json()

    def find(self, resource, col, value):
        value = value.replace("!", "!!").replace("'", "!'")   # rison escaping
        q = f"(filters:!((col:{col},opr:eq,value:'{value}')),page_size:100)"
        res = self.call("GET", f"{resource}/", params={"q": q})["result"]
        return res[0]["id"] if res else None


def metric(sql, label):
    return {"expressionType": "SQL", "sqlExpression": sql, "label": label}


def query_context(ds_id, p):
    """Stored query_context -> enables the Chart Data API, Alerts & Reports and cache warm-up."""
    vt = p["viz_type"]
    if vt == "big_number_total":
        q = {"metrics": [p["metric"]], "columns": []}
    elif vt == "pie":
        q = {"metrics": [p["metric"]], "columns": p["groupby"]}
    elif vt == "table":
        q = {"metrics": [], "columns": p["all_columns"]}
    else:
        axis = {"columnType": "BASE_AXIS", "sqlExpression": p["x_axis"], "label": p["x_axis"],
                "expressionType": "SQL", "timeGrain": p["time_grain_sqla"]}
        q = {"metrics": p["metrics"], "columns": [axis, *p["groupby"]], "orderby": [[p["metrics"][0], False]]}
    q.update({"filters": [], "extras": {"having": "", "where": ""}, "row_limit": p.get("row_limit", 10000)})
    return {"datasource": {"id": ds_id, "type": "table"}, "force": False, "queries": [q], "form_data": p,
            "result_format": "json", "result_type": "full"}


def main():
    sp = Superset()
    db_id = sp.find("database", "database_name", DB_NAME)
    if not db_id:
        raise SystemExit("Database 'Lakehouse' not found (it is created by the Superset bootstrap)")

    datasets = {}
    for t in ["estate_summary", "tenant_360", "procurement_monthly", "bus_daily", "asset_condition",
              "document_catalog", "tenant_tickets", "weighbridge_daily"]:
        ds = sp.find("dataset", "table_name", t)
        if not ds:
            ds = sp.call("POST", "dataset/", json={"database": db_id, "schema": "gold", "table_name": t})["id"]
        else:
            sp.call("PUT", f"dataset/{ds}/refresh")
        datasets[t] = ds

    dash_id = sp.find("dashboard", "dashboard_title", TITLE)
    if not dash_id:
        dash_id = sp.call("POST", "dashboard/", json={"dashboard_title": TITLE, "slug": "executive",
                                                     "published": True})["id"]

    charts = [  # (name, dataset, params, width)
        ("Active tenants", "estate_summary",
         {"viz_type": "big_number_total", "metric": metric("MAX(active_tenants)", "Active tenants")}, 3),
        ("Land occupancy (%)", "estate_summary",
         {"viz_type": "big_number_total", "metric": metric("MAX(land_occupancy_pct)", "Occupancy %")}, 3),
        ("Total lease value (IDR)", "estate_summary",
         {"viz_type": "big_number_total", "metric": metric("MAX(total_lease_value_idr)", "IDR"),
          "y_axis_format": "SMART_NUMBER"}, 3),
        ("Open tenant tickets", "estate_summary",
         {"viz_type": "big_number_total", "metric": metric("MAX(open_tickets)", "Tickets")}, 3),
        ("Procurement value per month", "procurement_monthly",
         {"viz_type": "echarts_timeseries_bar", "x_axis": "month", "time_grain_sqla": "P1M",
          "metrics": [metric("SUM(total_amount_idr)", "PO value")], "groupby": ["category"],
          "stack": "Stack", "row_limit": 10000, "y_axis_format": "SMART_NUMBER", "show_legend": True}, 8),
        ("Asset condition", "asset_condition",
         {"viz_type": "pie", "groupby": ["condition"], "metric": metric("SUM(asset_count)", "Assets"),
          "show_legend": True, "show_labels": True}, 4),
        ("E-bus km travelled per day", "bus_daily",
         {"viz_type": "echarts_timeseries_line", "x_axis": "date", "time_grain_sqla": "P1D",
          "metrics": [metric("SUM(km_travelled)", "Km")], "groupby": ["bus_id"], "row_limit": 10000,
          "show_legend": True}, 6),
        ("E-bus energy efficiency (kWh/km)", "bus_daily",
         {"viz_type": "echarts_timeseries_bar", "x_axis": "date", "time_grain_sqla": "P1D",
          "metrics": [metric("AVG(kwh_per_km)", "kWh/km")], "groupby": [], "row_limit": 10000}, 6),
        ("Tenant 360 (PII masked by policy)", "tenant_360",
         {"viz_type": "table", "query_mode": "raw", "row_limit": 100,
          "all_columns": ["tenant_id", "tenant_name", "sector", "plot_id", "area_ha", "contract_value_idr",
                          "open_tickets", "trucks_30d", "tonnage_30d", "pic_name", "pic_phone",
                          "pic_email"]}, 12),
        ("Document catalog (text & OCR)", "document_catalog",
         {"viz_type": "table", "query_mode": "raw", "row_limit": 100,
          "all_columns": ["file_name", "doc_type", "doc_number", "tenant_id", "doc_date",
                          "amount_idr", "extraction_method", "needs_review"]}, 12),
    ]

    layout = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
        "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": TITLE}},
    }
    row, row_w, rows = None, 0, 0
    for i, (name, ds, params, width) in enumerate(charts):
        params = {**params, "datasource": f"{datasets[ds]}__table", "adhoc_filters": []}
        payload = {"slice_name": name, "viz_type": params["viz_type"], "datasource_id": datasets[ds],
                   "datasource_type": "table", "params": json.dumps(params), "dashboards": [dash_id],
                   "query_context": json.dumps(query_context(datasets[ds], params)),
                   "query_context_generation": True}
        cid = sp.find("chart", "slice_name", name)
        if cid:
            sp.call("PUT", f"chart/{cid}", json=payload)
        else:
            cid = sp.call("POST", "chart/", json=payload)["id"]
        if row is None or row_w + width > 12:
            rows += 1
            row, row_w = f"ROW-{rows}", 0
            layout[row] = {"type": "ROW", "id": row, "children": [], "parents": ["ROOT_ID", "GRID_ID"],
                           "meta": {"background": "BACKGROUND_TRANSPARENT"}}
            layout["GRID_ID"]["children"].append(row)
        key = f"CHART-{i}"
        layout[key] = {"type": "CHART", "id": key, "children": [], "parents": ["ROOT_ID", "GRID_ID", row],
                       "meta": {"width": width, "height": 25 if params["viz_type"] == "big_number_total" else 60,
                                "chartId": cid, "sliceName": name}}
        layout[row]["children"].append(key)
        row_w += width

    sp.call("PUT", f"dashboard/{dash_id}", json={"position_json": json.dumps(layout), "published": True})
    print(f"Dashboard ready: {BASE}/superset/dashboard/executive/  ({len(charts)} charts, {len(datasets)} datasets)")


if __name__ == "__main__":
    main()
