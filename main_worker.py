import os
import time
from datetime import datetime

from config import APPTITLE, APITOKEN, PGDSN, SCHEMA
from yougile_api import YougileClient
from db import connect, ensureschema, upsertrows, getexistingids
from mapping import mapboard, mapuser, maptask

def run_sync_once():
    if not APITOKEN or not PGDSN:
        raise RuntimeError("Missing YOUGILE_API_TOKEN or DATABASE_URL")

    client = YougileClient(APITOKEN)

    conn = connect(PGDSN)
    ensureschema(conn, SCHEMA)

    # При необходимости получите существующие id (если в вашей логике это использовалось)
    _ = getexistingids(conn, "boards", SCHEMA)
    _ = getexistingids(conn, "users", SCHEMA)
    _ = getexistingids(conn, "tasks", SCHEMA)

    # 1:1 последовательность обращений
    boards = client.list_boards() or []
    users = client.list_users() or []
    tasks = client.list_tasks() or []

    # Преобразования — те же функции
    boardrows = []
    for b in boards:
        mb = mapboard(b)
        if mb:
            boardrows.append(mb)

    userrows = []
    for u in users:
        mu = mapuser(u)
        if mu:
            userrows.append(mu)

    taskrows = []
    for t in tasks:
        mt = maptask(t)
        if mt:
            taskrows.append(mt)

    # Запись данных — те же поля
    if boardrows:
        upsertrows(conn, "boards", ["id", "name"], boardrows, SCHEMA)
    if userrows:
        upsertrows(conn, "users", ["id", "name"], userrows, SCHEMA)
    if taskrows:
        upsertrows(
            conn,
            "tasks",
            [
                "id",
                "title",
                "boardid",
                "assigneeid",
                "createdat",
                "actualtime",
                "sprintname",
                "projectname",
                "direction",
                "statecategory",
            ],
            taskrows,
            SCHEMA,
        )

    conn.close()
