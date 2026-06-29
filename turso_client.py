import requests


class TursoClient:
    """Turso HTTP API (/v2/pipeline) を薄くラップする同期クライアント。"""

    def __init__(self, url: str, auth_token: str):
        # libsql:// と https:// の両形式を受け付ける
        self.base_url = url.replace("libsql://", "https://").rstrip("/")
        self.headers = {
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json",
        }

    def execute(self, sql: str, args: list | None = None) -> dict:
        return self._pipeline([{"sql": sql, "args": args or []}])

    def batch(self, statements: list[dict]) -> dict:
        return self._pipeline(statements)

    def query(self, sql: str, args: list | None = None) -> list[dict]:
        result = self._pipeline([{"sql": sql, "args": args or []}])
        rs = result["results"][0]
        if rs["type"] == "error":
            raise RuntimeError(rs["error"]["message"])
        cols = [c["name"] for c in rs["response"]["result"]["cols"]]
        return [
            {cols[i]: self._decode(v) for i, v in enumerate(row)}
            for row in rs["response"]["result"]["rows"]
        ]

    def _pipeline(self, statements: list[dict]) -> dict:
        body = []
        for stmt in statements:
            body.append({
                "type": "execute",
                "stmt": {
                    "sql": stmt["sql"],
                    "args": [self._encode(a) for a in (stmt.get("args") or [])],
                },
            })
        body.append({"type": "close"})

        resp = requests.post(
            f"{self.base_url}/v2/pipeline",
            headers=self.headers,
            json={"requests": body},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def _encode(val) -> dict:
        if val is None:
            return {"type": "null"}
        if isinstance(val, bool):
            return {"type": "integer", "value": "1" if val else "0"}
        if isinstance(val, int):
            return {"type": "integer", "value": str(val)}
        if isinstance(val, float):
            return {"type": "float", "value": val}
        return {"type": "text", "value": str(val)}

    @staticmethod
    def _decode(val) -> int | float | str | None:
        t = val["type"]
        if t == "null":
            return None
        if t == "integer":
            return int(val["value"])
        if t == "float":
            return float(val["value"])
        return val["value"]
