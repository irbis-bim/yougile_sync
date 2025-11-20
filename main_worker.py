from config import APITOKEN, PGDSN, SCHEMA
from yougile_api import YougileClient
from db import connect, ensure_schema, upsert_rows, get_existing_ids
from mapping import map_board, map_user, map_task

def run_sync_once():
    if not APITOKEN or not PGDSN:
        raise RuntimeError("Missing YOUGILE_API_TOKEN or DATABASE_URL")

    client = YougileClient(APITOKEN)
    conn = connect(PGDSN)
    ensure_schema(conn, SCHEMA)

    boards = client.list_boards() or []
    users = client.list_users() or []
    tasks = client.list_tasks() or []

    boardrows = [map_board(b) for b in boards if map_board(b)]
    userrows = [map_user(u) for u in users if map_user(u)]
    taskrows = [map_task(t) for t in tasks if map_task(t)]

    if boardrows:
        upsert_rows(conn, "boards", ["id", "name"], boardrows, SCHEMA)
    if userrows:
        upsert_rows(conn, "users", ["id", "name"], userrows, SCHEMA)
    if taskrows:
        # Важно: порядок полей должен совпадать с возвращаемым кортежем map_task
        # Сейчас map_task возвращает 11 полей, а ниже требуется 10
        # Нужно адаптировать под фактический порядок map_task или переделать map_task
        upsert_rows(conn, "tasks",
                   ["id","title","board_id","archived","completed","createdBy","assignee_id","created_at","updated_at","deadline","actual_time"],
                   taskrows, SCHEMA)

    conn.close()
