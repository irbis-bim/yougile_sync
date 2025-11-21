import requests
from urllib.parse import urljoin
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
import time

API_BASE = "https://ru.yougile.com/api-v2/"

class YougileError(Exception):
    pass

def _auth_headers(api_bearer_token: str) -> dict:
    return {
        "Authorization": f"Bearer {api_bearer_token}",
        "Content-Type": "application/json"
    }

@retry(wait=wait_exponential(multiplier=2, min=2, max=60),
       stop=stop_after_attempt(10),
       retry=retry_if_exception_type((requests.RequestException, YougileError)))
def _get(url: str, headers: dict, params: dict | None = None) -> dict | None:
    r = requests.get(url, headers=headers, params=params, timeout=30)
    if r.status_code == 429:
        raise YougileError("Rate limited (429). Retrying...")
    if r.status_code == 401:
        raise YougileError("Unauthorized. Проверьте Bearer-токен.")
    if r.status_code == 404:
        return None
    if not r.ok:
        raise YougileError(f"HTTP {r.status_code}: {r.text[:200]}")
    return r.json()

class YougileClient:
    def __init__(self, api_bearer_token: str):
        self.base_url = API_BASE
        self.headers = _auth_headers(api_bearer_token)

    def _list_paginated(self, endpoint: str) -> list[dict]:
        """Получить список с пагинацией, обработка 429 с задержками"""
        items = []
        page = 0
        page_size = 200
        
        while True:
            params = {"offset": page * page_size, "limit": page_size}
            try:
                data = _get(urljoin(self.base_url, endpoint), self.headers, params=params)
                if not data:
                    break
                batch = data.get("content", [])
                items.extend(batch)
                if len(batch) < page_size:
                    break
                time.sleep(0.5)  # Пауза между запросами
                page += 1
            except Exception as e:
                print(f"Error fetching {endpoint} page {page}: {e}")
                raise
        
        return items

    def list_boards(self) -> list[dict]:
        return self._list_paginated("boards")

    def list_users(self) -> list[dict]:
        return self._list_paginated("users")

    def list_columns(self) -> list[dict]:
        return self._list_paginated("columns")

    def list_tasks(self) -> list[dict]:
        return self._list_paginated("task-list")

    def get_all_sticker_states(self) -> dict[str, tuple[str, str, str]]:
        """Возвращает {state_id: (state_name, parent_id, parent_name)}"""
        states_map = {}
        for endpoint in ["string-stickers", "sprint-stickers"]:
            data = _get(urljoin(self.base_url, endpoint), self.headers)
            if data and "content" in data:
                for group in data["content"]:
                    if isinstance(group, dict):
                        parent_id = str(group.get("id", ""))
                        parent_name = str(group.get("name", ""))
                        for state in group.get("states", []):
                            if "id" in state and "name" in state:
                                state_id = str(state["id"])
                                state_name = str(state["name"])
                                states_map[state_id] = (state_name, parent_id, parent_name)
            time.sleep(0.5)
        return states_map
