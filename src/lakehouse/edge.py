"""Simulated on-site edge server: pulls from source systems and ships data to the landing zone.

- ERP / master data (Postgres): watermark-based CDC on updated_at -> one JSONL file per batch
- Daily files (weighbridge CSV, ticketing JSONL) -> landing zone
- PDF documents & photos -> raw Volume in the lakehouse (equivalent of Unity Catalog Volumes)
"""
import json
import os
from datetime import date, datetime
from decimal import Decimal

from lakehouse.common import LAKE_BUCKET, LANDING_BUCKET, SOURCES_DIR, ensure_buckets, pg, s3

ERP_TABLES = ["tenant", "plot", "asset", "vendor", "purchase_order", "employee"]
STATE_KEY = "_state/erp_watermark.json"


def _json_default(v):
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    raise TypeError(type(v))


def _existing_keys(client, bucket, prefix):
    keys = set()
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys.update(o["Key"] for o in page.get("Contents", []))
    return keys


def extract_erp(client) -> dict:
    try:
        state = json.loads(client.get_object(Bucket=LANDING_BUCKET, Key=STATE_KEY)["Body"].read())
    except client.exceptions.NoSuchKey:
        state = {}
    batch = datetime.now().strftime("%Y%m%dT%H%M%S")
    stats = {}
    with pg() as conn:
        cur = conn.cursor()
        for table in ERP_TABLES:
            wm = state.get(table, "1900-01-01T00:00:00")
            cur.execute(f"SELECT * FROM {table} WHERE updated_at > %s ORDER BY updated_at", (wm,))
            cols = [c.name for c in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            stats[f"erp.{table}"] = len(rows)
            if not rows:
                continue
            body = "\n".join(json.dumps(r, default=_json_default) for r in rows) + "\n"
            client.put_object(Bucket=LANDING_BUCKET, Key=f"erp/{table}/batch_{batch}.jsonl", Body=body.encode())
            state[table] = rows[-1]["updated_at"].isoformat()
    client.put_object(Bucket=LANDING_BUCKET, Key=STATE_KEY, Body=json.dumps(state).encode())
    return stats


def sync_dir(client, local_dir, bucket, prefix) -> int:
    src = os.path.join(SOURCES_DIR, local_dir)
    if not os.path.isdir(src):
        return 0
    have = _existing_keys(client, bucket, prefix)
    sent = 0
    for name in sorted(os.listdir(src)):
        key = f"{prefix}{name}"
        if key not in have:
            client.upload_file(os.path.join(src, name), bucket, key)
            sent += 1
    return sent


def run() -> dict:
    ensure_buckets()
    client = s3()
    stats = extract_erp(client)
    stats["file.weighbridge"] = sync_dir(client, "weighbridge", LANDING_BUCKET, "weighbridge/")
    stats["file.ticketing"] = sync_dir(client, "ticketing", LANDING_BUCKET, "ticketing/")
    stats["volume.documents"] = sync_dir(client, "documents", LAKE_BUCKET, "volumes/raw/documents/")
    stats["volume.photos"] = sync_dir(client, "photos", LAKE_BUCKET, "volumes/raw/photos/")
    return stats


if __name__ == "__main__":
    print(run())
