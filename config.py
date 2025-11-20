import os

APITOKEN = os.getenv("YOUGILE_API_TOKEN", "")
PGDSN = os.getenv("DATABASE_URL", "")
SCHEMA = os.getenv("PG_SCHEMA", "public")
