from datetime import datetime

def parsedt(v):
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

def hours(value):
    if value is None:
        return None
    try:
        val = float(value)
        if val == 0:
            return None
        return round(val, 1)
    except (ValueError, TypeError):
        return None

def mapboard(b: dict):
    # имя доски: берём name, как в исходнике
    name = b.get("name") or ""
    return (str(b.get("id")), str(name))

def mapuser(u: dict):
    # имя пользователя: realName приоритетно, затем name
    return (str(u.get("id")), str(u.get("realName") or u.get("name") or ""))

def maptask(t: dict):
    """
    Возвращает tuple под апсерт в таблицу tasks:
    ("id","title","board_id","assignee_id","created_at","actual_time",
     "sprint_name","project_name","direction","state_category")
    """
    tid = str(t.get("id") or "")
    title = (t.get("title") or "").strip()

    # boardId обязателен
    board_id = t.get("boardId") or t.get("board_id")
    board_id = str(board_id or "").strip()
    if not tid or not board_id:
        return None

    # assignee_id (по факту в исходной логике это один id)
    assignee_id = None
    assigned = t.get("assigned") or []
    if assigned:
        assignee_id = str(assigned[0])

    # created_at: из timestamp
    created_at = None
    ts = t.get("timestamp")
    if ts:
        try:
            if isinstance(ts, (int, float)):
                created_at = datetime.fromtimestamp(ts / 1000.0).date()
            else:
                created_at = datetime.fromisoformat(str(ts).replace("Z", "+00:00")).date()
        except Exception:
            created_at = None

    # фактическое время
    actual_time = None
    tt = t.get("timeTracking") or {}
    if "work" in tt:
        try:
            w = float(tt.get("work"))
            actual_time = round(w, 1) if w != 0 else None
        except (TypeError, ValueError):
            actual_time = None

    # поля-метки (заполнятся в будущем или останутся None, как в исходной схеме)
    sprint_name = None
    project_name = None
    direction = None
    state_category = None

    return (
        tid, title, board_id, assignee_id, created_at,
        actual_time, sprint_name, project_name, direction, state_category
    )
