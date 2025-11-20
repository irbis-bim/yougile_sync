import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def _parse_dt(v):
    try:
        if not v:
            return None
        if isinstance(v, dict) and "deadline" in v:
            return datetime.fromtimestamp(v["deadline"] / 1000.0)
        if isinstance(v, (int, float)) and v > 100000:
            return datetime.fromtimestamp(v / 1000.0)
        return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except Exception:
        return None

def _hours(value):
    if value is None:
        return None
    try:
        val = float(value)
        if val < 0:
            return None
        return round(val, 1)
    except (ValueError, TypeError):
        return None

def map_board(b: dict):
    return (str(b.get("id")), str(b.get("title") or ""))

def map_user(u: dict):
    return (str(u.get("id")), str(u.get("realName") or u.get("name") or ""))

def map_task(t: dict):
    """Возвращает кортеж или None если не подходит"""
    task_id = str(t.get("id") or "")
    
    # Попытка найти board_id из разных полей
    board_id = str(t.get("projectId") or t.get("boardId") or "").strip()
    
    # ЛОГИРОВАНИЕ для первой попытки
    if not task_id.startswith("test"):  # только для первых нескольких задач, чтобы не спамить логи
        logger.info(f"Task {task_id[:8]}: projectId={t.get('projectId')}, boardId={t.get('boardId')}, board_id={board_id}, keys={list(t.keys())[:5]}")
    
    if not board_id:
        logger.warning(f"Task {task_id} has no projectId/boardId, skipping")
        return None
    
    title = str(t.get("title") or "").strip()
    
    assignee_id = None
    assigned = t.get("assigned") or []
    if assigned:
        assignee_id = str(assigned[0])
    
    actual_time = _hours((t.get("timeTracking") or {}).get("work"))
    created_at = _parse_dt(t.get("timestamp"))
    
    result = (
        task_id, title, board_id, assignee_id, created_at,
        actual_time, None, None, None, None
    )
    
    return result
