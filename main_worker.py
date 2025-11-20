import logging
from config import APITOKEN, PGDSN, SCHEMA
from yougile_api import YougileClient
from db import connect, ensure_schema, upsert_rows, get_existing_ids
from mapping import map_board, map_user, map_task

logging.basicConfig(level=logging.DEBUG)
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
    tasks = client.list_tasks() or []

    logger.info(f"boards raw count = {len(boards)}")
    logger.info(f"users raw count = {len(users)}")
    logger.info(f"tasks raw count = {len(tasks)}")
    
    if boards:
        logger.info(f"first board sample: {boards[0]}")
    
    boardrows = [map_board(b) for b in boards if map_board(b)]
    userrows = [map_user(u) for u in users if map_user(u)]
    taskrows = [map_task(t) for t in tasks if map_task(t)]

    logger.info(f"boardrows count after map = {len(boardrows)}")
    logger.info(f"userrows count after map = {len(userrows)}")
    logger.info(f"taskrows count after map = {len(taskrows)}")
    
    if boardrows:
        logger.info(f"first boardrow sample: {boardrows[0]}")
    if taskrows:
        logger.info(f"first taskrow sample: {taskrows[0]}")

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
