"""Lakehouse Data API (for the operations centre & tenant apps).

Each API key maps to a Trino identity, so the row filters & column masks in rules.json apply
automatically -> the API does not duplicate any security logic.
Production: replace static API keys with OAuth2/OIDC (e.g. Keycloak brokering your IdP).
"""
import json
import os
import time

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import Response

from lakehouse.common import LAKE_BUCKET, s3, trino

API_KEYS: dict[str, str] = json.loads(os.environ["API_KEYS_JSON"])

app = FastAPI(title="Lakehouse Data API", version="0.1.0",
              description="Data API on top of the lakehouse. The `X-API-Key` header determines identity & data access.")


def identity(x_api_key: str | None = Header(None, description="e.g. demo-exec-key / demo-tenant-t003-key")) -> str:
    user = API_KEYS.get(x_api_key or "")
    if not user:
        raise HTTPException(401, "Invalid API key")
    return user


def query(user: str, sql: str, params: tuple = ()) -> list[dict]:
    cur = trino(user).cursor()
    try:
        cur.execute(sql, params)
        rows = cur.fetchall()
    except Exception as e:  # noqa: BLE001
        msg = str(e)
        if "Access Denied" in msg:
            raise HTTPException(403, "Access denied by data policy") from None
        raise HTTPException(500, msg[:300]) from None
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in rows]


@app.get("/health")
def health():
    t0 = time.time()
    query("admin", "SELECT 1")
    return {"status": "ok", "trino_ms": round((time.time() - t0) * 1000)}


@app.get("/v1/whoami")
def whoami(user: str = Depends(identity)):
    return {"user": user}


@app.get("/v1/summary")
def summary(user: str = Depends(identity)):
    return query(user, "SELECT * FROM delta.gold.estate_summary")


@app.get("/v1/tenants")
def tenants(user: str = Depends(identity)):
    return query(user, "SELECT * FROM delta.gold.tenant_360 ORDER BY tenant_id")


@app.get("/v1/tickets")
def tickets(status: str | None = Query(None, examples=["OPEN"]), user: str = Depends(identity)):
    if status:
        return query(user, "SELECT * FROM delta.gold.tenant_tickets WHERE status = ? ORDER BY created_at DESC",
                     (status,))
    return query(user, "SELECT * FROM delta.gold.tenant_tickets ORDER BY created_at DESC LIMIT 200")


@app.get("/v1/bus/daily")
def bus_daily(days: int = Query(7, ge=1, le=90), user: str = Depends(identity)):
    return query(user, "SELECT * FROM delta.gold.bus_daily WHERE date >= current_date - interval '1' day * ? "
                       "ORDER BY date DESC, bus_id", (days,))


@app.get("/v1/bus/latest-position")
def bus_latest_position(user: str = Depends(identity)):
    return query(user, "SELECT bus_id, max_by(lat, ts) lat, max_by(lon, ts) lon, max_by(soc_pct, ts) soc_pct, "
                       "max(ts) ts FROM delta.silver.bus_telemetry GROUP BY bus_id ORDER BY bus_id")


@app.get("/v1/documents")
def documents(q: str | None = Query(None, description="full-text search over extracted / OCR text",
                                    examples=["handover"]),
              user: str = Depends(identity)):
    cols = ("file_name, doc_type, doc_number, tenant_id, tenant_name, plot_id, doc_date, "
            "amount_idr, extraction_method, needs_review, summary")
    if q:
        return query(user, f"SELECT {cols} FROM delta.gold.document_catalog WHERE lower(text) LIKE ? "
                           "ORDER BY doc_date DESC", (f"%{q.lower()}%",))
    return query(user, f"SELECT {cols} FROM delta.gold.document_catalog ORDER BY doc_date DESC")


@app.get("/v1/photos")
def photos(asset_id: str | None = None, user: str = Depends(identity)):
    if asset_id:
        return query(user, "SELECT * FROM delta.gold.asset_photos WHERE asset_id = ? ORDER BY taken_at DESC",
                     (asset_id,))
    return query(user, "SELECT * FROM delta.gold.asset_photos ORDER BY taken_at DESC LIMIT 100")


@app.get("/v1/photos/{file_name}/thumbnail", response_class=Response)
def photo_thumbnail(file_name: str, user: str = Depends(identity)):
    rows = query(user, "SELECT thumbnail_key FROM delta.gold.asset_photos WHERE file_name = ?", (file_name,))
    if not rows:
        raise HTTPException(404, "Photo not found or not permitted")
    obj = s3().get_object(Bucket=LAKE_BUCKET, Key=rows[0]["thumbnail_key"])
    return Response(obj["Body"].read(), media_type="image/jpeg")
