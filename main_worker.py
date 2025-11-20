from config import APITOKEN, PGDSN, SCHEMA
from yougile_api import YougileClient
from db import connect, ensure_schema, upsert_rows, get_existing_ids
from mapping import mapboard, mapuser, maptask

def run_sync_once():
    if not APITOKEN or not PGDSN:
        raise RuntimeError("Missing YOUGILE_API_TOKEN or DATABASE_URL")

    client = YougileClient(APITOKEN)
    conn = connect(PGDSN)
    ensureschema(conn, SCHEMA)

    boards = client.list_boards() or []
    users = client.list_users() or []
    tasks = client.list_tasks() or []

    boardrows = [mapboard(b) for b in boards if mapboard(b)]
    userrows = [mapuser(u) for u in users if mapuser(u)]
    taskrows = [maptask(t) for t in tasks if maptask(t)]

    if boardrows:
        upsertrows(conn, "boards", ["id", "name"], boardrows, SCHEMA)
    if userrows:
        upsertrows(conn, "users", ["id", "name"], userrows, SCHEMA)
    if taskrows:
        upsertrows(conn, "tasks",
                   ["id","title","board_id","assignee_id","created_at","actual_time",
                    "sprint_name","project_name","direction","state_category"],
                   taskrows, SCHEMA)

    conn.close()
