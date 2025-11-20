import os
import time
from datetime import datetime

from config import APITOKEN, PGDSN, SCHEMA
from yougile_api import YougileClient
from db import connect, ensureschema, upsertrows, getexistingids
from mapping import mapboard, mapuser, maptask  # используем существующее преобразование

def run_sync_once():
    if not APITOKEN or not PGDSN:
        raise RuntimeError("Missing YOUGILE_API_TOKEN or DATABASE_URL")

    client = YougileClient(APITOKEN)

    conn = connect(PGDSN)
    ensureschema(conn, SCHEMA)

    # Уже существующие записи (для апсертов это опционально, но логика не меняется)
    existing_board_ids = getexistingids(conn, "boards", SCHEMA)
    existing_user_ids = getexistingids(conn, "users", SCHEMA)
    existing_task_ids = getexistingids(conn, "tasks", SCHEMA)

    # 1:1 последовательность обращений к API
    boards = client.list_boards() or []
    users = client.list_users() or []
    # при необходимости используйте клиентские методы для колонок/стикеров/состояний
    tasks = client.list_tasks() or []

    # Преобразования — строго теми же функциями, что используются в приложении
    boardrows = [mapboard(b) for b in boards if mapboard(b)]
    userrows = [mapuser(u) for u in users if mapuser(u)]
    taskrows = [mt for t in tasks if (mt := maptask(t))]

    # Запись данных — те же наборы таблиц/полей
    if boardrows:
        upsertrows(conn, "boards", ["id", "name"], boardrows, SCHEMA)
    if userrows:
        upsertrows(conn, "users", ["id", "name"], userrows, SCHEMA)
    if taskrows:
        # Поля соответствуют вашей таблице tasks
        upsertrows(conn, "tasks",
                   ["id", "title", "boardid", "assigneeid", "createdat",
                    "actualtime", "sprintname", "projectname", "direction", "statecategory"],
                   taskrows, SCHEMA)

    conn.close()

if __name__ == "__main__":
    mode = os.getenv("WORKER_MODE", "once")  # once | loop
    if mode == "loop":
        interval = int(os.getenv("SYNC_INTERVAL_SEC", "900"))
        while True:
            run_sync_once()
            time.sleep(interval)
    else:
        run_sync_once()
