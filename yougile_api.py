import requests
from urllib.parse import urljoin
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

APIBASE = "https://ru.yougile.com/api-v2"

class YougileError(Exception):
    pass

def authheaders(api_bearer_token: str) -> dict:
    return {
        "Authorization": f"Bearer {api_bearer_token}",
        "Content-Type": "application/json",
    }

@retry(wait=wait_exponential(multiplier=1, min=1, max=30),
       stop=stop_after_attempt(5),
       retry=retry_if_exception_type((requests.RequestException, YougileError)))
def get(url: str, headers: dict, params: dict | None = None):
    """GET запрос с ретраями для обработки 429 (rate limit) и 401 (auth)"""
    r = requests.get(url, headers=headers, params=params, timeout=30)
    if r.status_code == 429:
        raise YougileError("Rate limited (429). Retrying...")
    if r.status_code == 401:
        raise YougileError("Unauthorized. Check Bearer token.")
    if r.status_code == 404:
        return None
    if not r.ok:
        raise YougileError(f"HTTP {r.status_code}: {r.text}")
    return r.json()

def _unpack(data):
    """Распаковать результат: если массив — вернуть его; если объект с items/data — вернуть оттуда"""
    if data is None:
        return []
    if isinstance(data, list):
        return data
    # YouGile v2 может возвращать {"items": [...]} или {"data": [...]}
    if isinstance(data, dict):
        items = data.get("items") or data.get("data")
        if items and isinstance(items, list):
            return items
    return []

def get_one(url: str, headers: dict):
    """Получить один запрос к эндпоинту (без пагинации page/pageSize, не поддерживаемых v2)"""
    resp = get(url, headers=headers, params=None)
    return _unpack(resp)

class YougileClient:
    def __init__(self, api_bearer_token: str):
        self.baseurl = APIBASE
        self.headers = authheaders(api_bearer_token)

    def list_boards(self) -> list[dict]:
        """Получить все доски"""
        url = urljoin(self.baseurl + "/", "boards")
        return get_one(url, self.headers)

    def list_users(self) -> list[dict]:
        """Получить всех пользователей"""
        url = urljoin(self.baseurl + "/", "users")
        return get_one(url, self.headers)

    def list_tasks(self) -> list[dict]:
        """Получить все задачи"""
        url = urljoin(self.baseurl + "/", "tasks")
        return get_one(url, self.headers)
