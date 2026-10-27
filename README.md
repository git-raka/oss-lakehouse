# Open-Source Lakehouse Simulation

An end-to-end lakehouse reference implementation (sources → edge server → Bronze → Silver → Gold → dashboards & API)
built **entirely from open-source components**, as an alternative to, or a benchmark for, a managed Databricks platform.

The scenario is a fictional **industrial estate**. It covers:
- **Structured data:** ERP, HR, tenants, assets, a weighbridge, ticketing, and IoT telemetry from an electric bus fleet.
- **Unstructured data:** PDF contracts and certificates, including scanned documents processed with OCR, and field photos processed for EXIF and GPS.

> All data in this repository is **synthetic** (produced by a generator). There is no real company or personal data.

## Data flow

```
SOURCES                 INGESTION (edge)          LAKEHOUSE (Delta Lake on SeaweedFS / S3)               CONSUMPTION
ERP/HR/tenants (PG) ─┐  CDC watermark ─┐                                                              ┌─ Superset (dashboards)
Weighbridge (CSV) ───┤  file sync ─────┼─> landing ─> BRONZE ─> SILVER ──────────────> GOLD ─> Trino ──┼─ Data API (FastAPI)
Ticketing (JSON) ────┤                 │   zone      (raw,      (clean, MERGE/CDC,      (ready  (row   └─ SQL / notebooks
E-bus fleet (MQTT) ──┤  Mosquitto ─────┘             incr.)     PDF OCR, photo EXIF)    to use) filters,
PDFs & photos ───────┘  ─────────────────────> VOLUME (original PDF/JPEG files + thumbnails)       masks)
                                  orchestration & data quality: Dagster (every 30 minutes)
```

## Component mapping (Databricks → open source)

| Capability | Databricks | This project | License |
|---|---|---|---|
| Object storage | Cloud storage | **SeaweedFS** (S3 API) | Apache 2.0 |
| Table format | Delta Lake | **Delta Lake 4.4** | Apache 2.0 |
| Processing / pipelines | Lakeflow | **Apache Spark 4.2** (PySpark, Structured Streaming) | Apache 2.0 |
| Incremental file ingest | Auto Loader | Structured Streaming `availableNow` + checkpoints | Apache 2.0 |
| File storage for unstructured data | Unity Catalog Volumes | `s3://lakehouse/volumes/` prefix | — |
| Job orchestration | Lakeflow Jobs | **Dagster** (lineage, schedules, asset checks) | Apache 2.0 |
| SQL warehouse | Serverless SQL | **Trino 484** | Apache 2.0 |
| Row filters / column masks | Unity Catalog | Trino file-based access control | Apache 2.0 |
| Dashboards | AI/BI | **Apache Superset 6.1** | Apache 2.0 |
| Data API | — | **FastAPI** | MIT |
| Document OCR | AI functions | **Tesseract**, pdfplumber, Poppler | Apache 2.0 / MIT / GPL |
| IoT broker | — | **Eclipse Mosquitto** (MQTT) | EPL 2.0 |
| Source ERP (simulated) | — | PostgreSQL 16 | PostgreSQL |

## Quick start

Requirements: Docker with Compose v2, `make`, about 6 GB of free RAM, and about 10 GB of disk.

```bash
cp .env.example .env      # then change every credential
make setup                # builds images, starts services, generates data, runs the pipeline, builds the dashboard
```

| Service | URL | Login |
|---|---|---|
| Superset (executive dashboard) | http://HOST:8088/superset/dashboard/executive/ | `admin` / `SUPERSET_ADMIN_PASSWORD` |
| Dagster (pipelines, lineage, data quality) | http://HOST:3001 | — |
| Data API (Swagger UI) | http://HOST:8000/docs | `X-API-Key` header (see `API_KEYS_JSON`) |
| JupyterLab (optional, `make notebook`) | http://HOST:8888 | `JUPYTER_TOKEN` |
| Trino / S3 / MQTT | bound to `127.0.0.1` only (internal) | — |

Run `make help` for the other commands.

## Demo scenarios

**1. Governance.** The same query returns different results depending on who runs it:
```bash
make governance
```
| User | What they see |
|---|---|
| `admin` | all rows, unmasked PII |
| `executive` (also used by Superset) | all tenants; phone, email and national ID masked |
| `analyst_finance` | silver and gold layers; PII masked; national IDs and salaries hidden |
| `tenant_t003` | **only tenant T003's rows**; access to HR data is denied |

**2. Data API.** Access follows the caller's identity in Trino:
```bash
curl -H 'X-API-Key: demo-exec-key'        http://HOST:8000/v1/tenants          # 12 tenants, PII masked
curl -H 'X-API-Key: demo-tenant-t003-key' http://HOST:8000/v1/tenants          # T003 only
curl -H 'X-API-Key: demo-tenant-t003-key' 'http://HOST:8000/v1/documents?q=handover'   # search OCR text
curl -H 'X-API-Key: demo-exec-key'        http://HOST:8000/v1/bus/latest-position
```

**3. CDC and incremental processing.** Change the source data, then run the pipeline. Only the changes are processed:
```bash
make increment && make pipeline
# e.g. purchase_order {'inserted': 15, 'updated': 20}, tenant {'updated': 1}, 2 new scanned PDFs OCR'd
```

**4. Time travel.** Query data as it was before a change, through Trino or Spark:
```sql
SELECT pic_phone FROM delta.silver.tenant FOR VERSION AS OF 0 WHERE tenant_id = 'T003';
SELECT * FROM delta.silver."purchase_order$history";
```

**5. Unstructured data.** See `notebooks/01_demo_lakehouse.ipynb`: scanned PDF → OCR → fields, and photo → EXIF/GPS.

## Repository layout

```
docker-compose.yml        all services, with memory limits and health checks
config/trino/security/    rules.json (row filters, column masks), groups.txt
src/lakehouse/
  seed.py                 synthetic source-data generator
  edge.py                 edge server: watermark CDC + file sync
  iot/                    e-bus simulator (MQTT) + collector into the landing zone
  bronze.py silver.py gold.py publish.py    medallion pipeline
  orchestration.py        Dagster assets, schedules, data-quality checks, maintenance
  api/main.py             Data API
  superset_setup.py       dashboard provisioning (idempotent)
notebooks/                exploration demo
```

## Resource usage (measured on an 8 vCPU / 16 GB VM)

At idle the stack uses about 2.5 GB of RAM: Trino 1.2 GB, Superset 0.5 GB, SeaweedFS 0.4 GB, and less than 0.3 GB for everything else.
Spark adds about 2 GB while the pipeline runs. A full run takes about 2 minutes on the simulated data, and a full install from scratch takes about 4 minutes.

## Production readiness

**Already implemented**
- Incremental, idempotent ingestion (checkpoints), CDC via `MERGE`, and "latest version wins" de-duplication.
- Data quality through Dagster asset checks: invalid-record ratio, IoT freshness, OCR review queue, and contract completeness.
- A `needs_review` queue for incomplete OCR results, and `parser_version` so files are reprocessed automatically when extraction logic changes.
- Row filters and column masks defined in one place (Trino) and enforced for both dashboards and the API.
- Time travel, plus scheduled `OPTIMIZE` / `VACUUM` with 7-day retention.
- Credentials supplied through the environment (none in the repo), internal ports not exposed, health checks, and memory limits.
- Dashboards provisioned as code (idempotent).

**Still needed for production**
| Area | Recommendation |
|---|---|
| Identity & SSO | Keycloak as a broker to your identity provider (e.g. Microsoft Entra ID with MFA); OAuth2 for Trino, Superset and the Data API |
| Encryption | TLS on every endpoint (a reverse proxy such as Traefik or Caddy) and MQTT over TLS |
| Availability | Kubernetes (k3s/RKE2) with the official Helm charts for Trino, Superset and Dagster; multi-node SeaweedFS with replication |
| Catalog & lineage | A shared catalog (Hive Metastore or Unity Catalog OSS); OpenLineage + Marquez or OpenMetadata |
| Access policies | Apache Ranger once the rule set grows (UI and audit trail) |
| Observability | Prometheus + Grafana (Trino JMX, Dagster metrics) and centralised logs (Loki) |
| Backup | Bucket replication to a second site (rclone/restic) and scheduled Postgres backups |
| Secrets | Vault or sealed-secrets, with credential rotation |
| Documents | Docling or PaddleOCR for complex layouts and tables; semantic search (pgvector or Qdrant) |
| Testing & CI/CD | Unit tests for transformations, schema contract tests, and a CI pipeline |
