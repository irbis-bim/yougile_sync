import logging
from datetime import datetime, date, timedelta

from config import API_TOKEN, PG_DSN, SCHEMA
from yougile_api import YougileClient
from db import connect, ensure_schema, upsert_rows, get_existing_ids

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфиг для конкретной доски (скопировано из app.py)
SPECIAL_BOARD_ID = "b3ca4ebc-858e-46b9-8d43-c34035fe9f07"
SPECIAL_PROJECT_STICKER_ID = "5b0a3b20-1dbb-4df5-b3e6-37ae4581905a"
SPECIAL_DIRECTION_STICKER_ID = "093eef50-9bde-4d5a-b790-b902e0d1d1b9"
DEFAULT_PROJECT_STICKER_ID = "c3e14cd1-7d09-437c-9fe2-e009fb8cd313"
DEFAULT_DIRECTION_STICKER_ID = "120b46c6-ffac-42cb-87b4-e914077e0404"


def _parse_dt(v):
    """Парсим дату как в app.py: из мс/сек/ISO в date."""
    try:
        if not v:
            return None
        if isinstance(v, (int, float)):
            if v > 100000000000:
                dt = datetime.fromtimestamp(v / 1000.0)
            else:
                dt = datetime.fromtimestamp(v)
            return dt.date()
        if isinstance(v, str):
            dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
            return dt.date()
        return None
    except (ValueError, TypeError, OSError):
        return None


def run_sync_once():
    """Синхронизация только 3‑месячного окна задач."""
    logger.info("Подключение к PostgreSQL…")
    conn = connect(PG_DSN)
    ensure_schema(conn, SCHEMA)
    logger.info(f"Схема '{SCHEMA}' готова.")

    # 1. Чистим только задачи за последние 90 дней
    cutoff_date = date.today() - timedelta(days=90)
    logger.info(f"Удаляем задачи из БД с created_at >= {cutoff_date}")
    with conn, conn.cursor() as cur:
        cur.execute(
            f"DELETE FROM {SCHEMA}.tasks WHERE created_at >= %s;",
            (cutoff_date,),
        )

    # 2. Получаем существующие ID (после очистки окна)
    logger.info("Проверка существующих данных…")
    existing_task_ids = get_existing_ids(conn, "tasks", SCHEMA)
    existing_board_ids = get_existing_ids(conn, "boards", SCHEMA)
    existing_user_ids = get_existing_ids(conn, "users", SCHEMA)
    logger.info(
        f"В БД уже: досок={len(existing_board_ids)}, "
        f"пользователей={len(existing_user_ids)}, "
        f"задач всего (старых)={len(existing_task_ids)}"
    )

    # 3. Загрузка из API
    logger.info("Загрузка данных из API…")
    client = YougileClient(API_TOKEN)
    boards = client.list_boards() or []
    users_api = client.list_users() or []
    columns = client.list_columns() or []
    tasks_raw = client.list_tasks() or []
    logger.info(
        f"Получено: досок={len(boards)}, пользователей={len(users_api)}, "
        f"колонок={len(columns)}, задач всего={len(tasks_raw)}"
    )

    logger.info("Загрузка стикеров…")
    sticker_states = client.get_all_sticker_states()
    logger.info(f"Стикеров загружено: {len(sticker_states)}")

    # 4. Оставляем только задачи за последние 90 дней
    cutoff_dt = datetime.utcnow() - timedelta(days=90)
    filtered_tasks = []
    for t in tasks_raw:
        ts = t.get("createdAt") or t.get("timestamp")
        if not ts:
            continue
        try:
            if isinstance(ts, (int, float)):
                dt = datetime.fromtimestamp(ts / 1000.0) if ts > 100000000000 else datetime.fromtimestamp(ts)
            else:
                dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        except Exception:
            continue
        if dt >= cutoff_dt:
            filtered_tasks.append(t)
    tasks_raw = filtered_tasks
    logger.info(f"Задач в окне 90 дней: {len(tasks_raw)}")

    # --- Подготовка данных для досок (только новые) ---
    logger.info("Подготовка досок…")
    board_rows = [
        (str(b.get("id")), str(b.get("name") or b.get("title") or b.get("caption") or ""))
        for b in boards
        if b.get("id") and str(b.get("id")) not in existing_board_ids
    ]
    logger.info(f"Новых досок к загрузке: {len(board_rows)}")

    # --- Подготовка маппинга колонок -> доски ---
    col_to_board = {
        str(c.get("id")): str(c.get("boardId"))
        for c in columns
        if c.get("id") and c.get("boardId")
    }
    logger.info(f"Маппинг колонок: {len(col_to_board)} связей")

    # --- Собираем user_id из задач (в окне 90 дней) ---
    logger.info("Сбор пользователей из задач…")
    all_user_ids_from_tasks = set()
    for t in tasks_raw:
        assigned = t.get("assigned") or []
        for uid in assigned:
            if uid:
                all_user_ids_from_tasks.add(str(uid))

    # --- Подготовка данных для пользователей (только новые) ---
    logger.info("Подготовка пользователей…")
    user_rows = []
    processed_users = set(existing_user_ids)
    for u in users_api:
        uid = u.get("id")
        if uid:
            uid_str = str(uid)
            if uid_str not in existing_user_ids:
                user_rows.append((uid_str, str(u.get("realName") or u.get("name") or "")))
                processed_users.add(uid_str)
    for uid_str in all_user_ids_from_tasks:
        if uid_str not in processed_users:
            user_rows.append((uid_str, f"Unknown User {uid_str[:8]}"))
            processed_users.add(uid_str)
    logger.info(f"Новых пользователей к загрузке: {len(user_rows)}")

    # --- Сохранение досок ---
    if board_rows:
        logger.info("Сохранение новых досок…")
        upsert_rows(conn, "boards", ["id", "name"], board_rows, SCHEMA)

    # --- Сохранение пользователей ---
    if user_rows:
        logger.info("Сохранение новых пользователей…")
        upsert_rows(conn, "users", ["id", "name"], user_rows, SCHEMA)

    # --- Подготовка данных для задач (только окно 90 дней) ---
    logger.info("Подготовка задач…")
    task_rows = []
    skipped_tasks = 0
    for t in tasks_raw:
        task_id = t.get("id")
        if not task_id:
            continue

        # Пропускаем, если такая задача уже есть в БД (из старого окна)
        if str(task_id) in existing_task_ids:
            continue

        col_id = t.get("columnId")
        board_id = col_to_board.get(str(col_id)) if col_id else None
        if not board_id:
            skipped_tasks += 1
            continue

        title = str(t.get("title") or t.get("name") or "")

        assignee_id = None
        assigned = t.get("assigned") or []
        if assigned and len(assigned) > 0:
            assignee_id = str(assigned[0])

        created_at = _parse_dt(t.get("createdAt") or t.get("timestamp"))

        actual_time = None
        tt = t.get("timeTracking") or {}
        if "work" in tt:
            try:
                actual_time = float(tt["work"])
            except (ValueError, TypeError):
                pass

        # Логика по стикерам
        sprint_name = None
        project_name = None
        direction = None
        state_category = None

        if board_id == SPECIAL_BOARD_ID:
            project_sticker_id = SPECIAL_PROJECT_STICKER_ID
            direction_sticker_id = SPECIAL_DIRECTION_STICKER_ID
        else:
            project_sticker_id = DEFAULT_PROJECT_STICKER_ID
            direction_sticker_id = DEFAULT_DIRECTION_STICKER_ID

        stickers = t.get("stickers") or {}
        for state_id in stickers.values():
            state_id_str = str(state_id)
            if state_id_str in sticker_states:
                state_name_val, parent_id, parent_name = sticker_states[state_id_str]

                if parent_id == project_sticker_id:
                    project_name = state_name_val
                elif parent_id == direction_sticker_id:
                    direction = state_name_val

                name_lower = state_name_val.lower()
                if "спринт" in name_lower or "sprint" in name_lower:
                    sprint_name = state_name_val
                else:
                    state_category = parent_name

        task_rows.append(
            (
                str(task_id),
                title,
                board_id,
                assignee_id,
                created_at,
                actual_time,
                sprint_name,
                project_name,
                direction,
                state_category,
            )
        )

    logger.info(f"Новых задач к загрузке: {len(task_rows)} (пропущено без доски: {skipped_tasks})")

    # --- Сохранение задач ---
    if task_rows:
        logger.info("Сохранение новых задач…")
        upsert_rows(
            conn,
            "tasks",
            ["id", "title", "board_id", "assignee_id", "created_at", "actual_time",
             "sprint_name", "project_name", "direction", "state_category"],
            task_rows,
            SCHEMA,
        )

    logger.info("✓ Импорт завершён!")
    conn.close()
