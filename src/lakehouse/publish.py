"""Register the Delta tables (bronze/silver/gold) in the Trino catalog so they can be queried with SQL
and governed (row filters, column masks) by rules.json."""
from lakehouse.common import LAKE_BUCKET, LAYERS, s3, trino


def delta_tables(layer: str) -> list[str]:
    client = s3()
    out = []
    resp = client.list_objects_v2(Bucket=LAKE_BUCKET, Prefix=f"{layer}/", Delimiter="/")
    for p in resp.get("CommonPrefixes", []):
        name = p["Prefix"].rstrip("/").split("/")[-1]
        log = client.list_objects_v2(Bucket=LAKE_BUCKET, Prefix=f"{layer}/{name}/_delta_log/", MaxKeys=1)
        if log.get("KeyCount", 0) and not name.startswith("_"):
            out.append(name)
    return out


def run() -> dict:
    cur = trino("pipeline").cursor()
    stats = {}
    for layer in LAYERS:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS delta.{layer} WITH (location = 's3://{LAKE_BUCKET}/{layer}')")
        cur.fetchall()
        cur.execute(f"SHOW TABLES FROM delta.{layer}")
        registered = {r[0] for r in cur.fetchall()}
        new = []
        for table in delta_tables(layer):
            if table not in registered:
                cur.execute(f"CALL delta.system.register_table(schema_name => '{layer}', table_name => '{table}', "
                            f"table_location => 's3://{LAKE_BUCKET}/{layer}/{table}')")
                cur.fetchall()
                new.append(table)
        stats[layer] = {"registered": len(registered) + len(new), "new": new}
    return stats


if __name__ == "__main__":
    print(run())
