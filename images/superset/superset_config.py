import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://postgres:{os.environ['POSTGRES_PASSWORD']}@postgres:5432/superset"
)
TALISMAN_ENABLED = False
WTF_CSRF_ENABLED = True
BABEL_DEFAULT_LOCALE = "en"
FEATURE_FLAGS = {"DASHBOARD_RBAC": True}
