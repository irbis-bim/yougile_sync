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

class YougileClient:
    def __init__(self, api_bearer_token: str):
        self.baseurl = APIBASE
        self.headers = authheaders(api_bearer_token)

    def list_paginated(self, endpoint: str) -> list[dict]:
        items = []
        page, page_size = 0, 200
        while True:
            url = urljoin(self.baseurl + "/", endpoint.lstrip("/"))
            data = get(url, headers=self.headers, params={"page": page, "pageSize": page_size}) or {}
            chunk = data if isinstance(data, list) else data.get("items") or data.get("data") or []
            if not chunk:
                break
            items.extend(chunk)
            if len(chunk) < page_size:
                break
            page += 1
        return items

    def list_boards(self) -> list[dict]:
        return self.list_paginated("boards")

    def list_users(self) -> list[dict]:
        return self.list_paginated("users")

    def list_tasks(self) -> list[dict]:
        return self.list_paginated("tasks")
