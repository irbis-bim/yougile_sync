from config import APITOKEN, PGDSN, SCHEMA
from yougile_api import YougileClient
from db import connect, ensure_schema, upsert_rows, get_existing_ids
from mapping import map_board, map_user, map_task

import logging
logger = logging.getLogger(__name__)

def run_sync_once():
    if not APITOKEN or not PGDSN:
        raise RuntimeError("Missing YOUGILE_API_TOKEN or DATABASE_URL")

    logger.info("Starting sync...")
    
    client = YougileClient(APITOKEN)
    conn = connect(PGDSN)
    ensure_schema(conn, SCHEMA)

    boards = client.list_boards() or []
    users = client.list_users() or []
    columns = client.list_columns() or []  # ← добавить получение колонок
    tasks = client.list_tasks() or []

    # Создать маппинг columnId → boardId
    column_to_board = {c.get("id"): c.get("boardId") for c in columns}
    logger.info(f"column_to_board mapping: {column_to_board}")

    boardrows = [map_board(b) for b in boards if map_board(b)]
    userrows = [map_user(u) for u in users if map_user(u)]
    taskrows = [map_task(t, column_to_board) for t in tasks if map_task(t, column_to_board)]  # ← передать маппинг

    logger.info(f"boards count = {len(boardrows)}")
    logger.info(f"users count = {len(userrows)}")
    logger.info(f"tasks count = {len(taskrows)}")

    if boardrows:
        upsert_rows(conn, "boards", ["id", "name"], boardrows, SCHEMA)
    if userrows:
        upsert_rows(conn, "users", ["id", "name"], userrows, SCHEMA)
    if taskrows:
        upsert_rows(conn, "tasks",
                   ["id","title","board_id","assignee_id","created_at","actual_time",
                    "sprint_name","project_name","direction","state_category"],
                   taskrows, SCHEMA)

    logger.info("Sync completed successfully")
    conn.close()
