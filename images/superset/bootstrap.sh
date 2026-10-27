#!/bin/sh
set -e
superset db upgrade
superset fab create-admin --username admin --firstname Lakehouse --lastname Admin \
  --email admin@example.com --password "$SUPERSET_ADMIN_PASSWORD" || true
superset init
# Connect to the lakehouse through Trino as user "superset" (executive group: PII is masked)
superset set-database-uri -d "Lakehouse" -u "trino://superset@trino:8080/delta" || true
exec /usr/bin/run-server.sh
