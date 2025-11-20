import os

APPTITLE = os.getenv("APP_TITLE", "YouGile Sync")
APITOKEN = os.getenv("YOUGILE_API_TOKEN", "")
PGDSN = os.getenv("DATABASE_URL", "")
SCHEMA = os.getenv("PG_SCHEMA", "public")
HEADLESS = os.getenv("HEADLESS", "1") == "1"
