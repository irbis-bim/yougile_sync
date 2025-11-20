import os

API_TOKEN = os.getenv("YOUGILE_API_TOKEN", "")
PG_DSN = os.getenv("DATABASE_URL", "")
SCHEMA = os.getenv("PG_SCHEMA", "yougile")
