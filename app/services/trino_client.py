from typing import Any

class TrinoClient:
    def __init__(self, jdbc_url: str):
        # Expect jdbc://host:port/catalog?user=...&password=...&schema=...
        # Implement parsing & trino connection here if needed.
        self.jdbc_url = jdbc_url

    def query(self, sql: str) -> list[tuple[Any, ...]]:
        # Stub for contest: organizers provide connectivity in real runs.
        return []

    def sample_stats(self, full_table_name: str) -> dict:
        return {"table": full_table_name, "rows": 0, "size_bytes": 0}
