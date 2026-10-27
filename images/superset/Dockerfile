FROM apache/superset:6.1.0
USER root
RUN uv pip install --no-cache --python /app/.venv/bin/python trino psycopg2-binary
COPY superset_config.py /app/pythonpath/superset_config.py
COPY bootstrap.sh /app/bootstrap.sh
RUN chmod +x /app/bootstrap.sh
USER superset
CMD ["/app/bootstrap.sh"]
