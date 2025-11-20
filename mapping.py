from datetime import datetime

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
    return (str(b.get("id")), str(b.get("name") or ""))

def map_user(u: dict):
    return (str(u.get("id")), str(u.get("realName") or u.get("name") or ""))

def map_string_sticker(s: dict):
    return (str(s.get("id")), str(s.get("name") or ""))

def map_string_state(sticker_id: str, st: dict):
    return (str(st.get("id")), sticker_id, str(st.get("name") or ""))

def map_sprint_sticker(s: dict):
    return (str(s.get("id")), str(s.get("name") or ""))

def map_sprint_state(sticker_id: str, st: dict):
    return (str(st.get("id")), sticker_id, str(st.get("name") or ""), st.get("begin"), st.get("end"))

def map_task(t: dict):
    """Маппинг задачи. Возвращает кортеж или None если boardId пустой"""
    board_id = str(t.get("boardId") or "").strip()
    
    # Пропускаем задачи без boardId
    if not board_id:
        return None
    
    assignee_id = None
    assigned = t.get("assigned") or []
    if assigned:
        assignee_id = str(assigned[0])
    
    tt = t.get("timeTracking") or {}
    actual_time = _hours(tt.get("work"))
    
    return (
        str(t.get("id")),
        str(t.get("title") or ""),
        board_id,
        bool(t.get("archived") or False),
        bool(t.get("completed") or False),
        str(t.get("createdBy") or ""),
        assignee_id,
        _parse_dt(t.get("timestamp")),
        _parse_dt(t.get("timestamp")),
        _parse_dt(t.get("deadline")),
        actual_time
    )

def split_task_stickers(t: dict):
    """Разбить стикеры задачи на пары (task_id, sticker_id, state_id)"""
    task_id = str(t.get("id"))
    pairs = []
    stickers = t.get("stickers") or {}
    for sticker_id, state_id in stickers.items():
        pairs.append((task_id, str(sticker_id), str(state_id)))
    return pairs
