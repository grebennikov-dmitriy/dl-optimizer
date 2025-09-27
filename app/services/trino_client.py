from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlparse

from trino import dbapi
from trino.auth import BasicAuthentication


@dataclass
class _TrinoParams:
    host: str
    port: int
    catalog: Optional[str]
    schema: Optional[str]
    user: str
    password: Optional[str]
    http_scheme: str
    session_properties: Dict[str, str]


class TrinoClient:
    def __init__(self, jdbc_url: str):
        self.jdbc_url = jdbc_url
        self.params = self._parse_jdbc_url(jdbc_url)

    def query(self, sql: str) -> list[tuple[Any, ...]]:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            cur.close()
            return rows
        finally:
            conn.close()

    def sample_stats(self, full_table_name: str) -> dict:
        stats: Dict[str, Any] = {"table": full_table_name}
        try:
            rows = self.query(f"SELECT count(*) AS row_count FROM {full_table_name}")
            stats["row_count"] = rows[0][0] if rows else 0
        except Exception as exc:  # pragma: no cover - network dependent
            stats["row_count_error"] = str(exc)
        return stats

    def _connect(self):
        auth = None
        if self.params.password:
            auth = BasicAuthentication(self.params.user, self.params.password)

        return dbapi.connect(
            host=self.params.host,
            port=self.params.port,
            user=self.params.user,
            catalog=self.params.catalog,
            schema=self.params.schema,
            http_scheme=self.params.http_scheme,
            auth=auth,
            session_properties=self.params.session_properties or None,
        )

    @staticmethod
    def _parse_jdbc_url(jdbc_url: str) -> _TrinoParams:
        if not jdbc_url:
            raise ValueError("Empty JDBC URL")

        parsed = urlparse(jdbc_url)
        if parsed.scheme.lower() != "jdbc":
            raise ValueError(f"Unsupported JDBC scheme: {parsed.scheme}")

        if not parsed.netloc and parsed.path:
            normalized_path = parsed.path
            lowered_path = normalized_path.lower()
            for prefix in ("trino://", "presto://"):
                if lowered_path.startswith(prefix):
                    remainder = normalized_path[len(prefix) :]
                    reparsed = urlparse(f"jdbc://{remainder}")
                    parsed = parsed._replace(netloc=reparsed.netloc, path=reparsed.path)
                    break

        if not parsed.hostname:
            raise ValueError("JDBC URL must contain host")

        path_parts = [p for p in parsed.path.split("/") if p]
        catalog = path_parts[0] if path_parts else None
        schema_from_path = path_parts[1] if len(path_parts) > 1 else None

        query_params = parse_qs(parsed.query)
        user = TrinoClient._single_param(query_params, "user") or TrinoClient._single_param(query_params, "username")
        if not user:
            raise ValueError("JDBC URL must contain user parameter")

        password = TrinoClient._single_param(query_params, "password")
        schema = TrinoClient._single_param(query_params, "schema") or schema_from_path
        http_scheme = "https" if TrinoClient._is_https(query_params) else "http"
        session_props = TrinoClient._parse_session_properties(query_params)

        port = parsed.port or (443 if http_scheme == "https" else 8080)

        return _TrinoParams(
            host=parsed.hostname,
            port=port,
            catalog=catalog,
            schema=schema,
            user=user,
            password=password,
            http_scheme=http_scheme,
            session_properties=session_props,
        )

    @staticmethod
    def _single_param(params: Dict[str, list[str]], key: str) -> Optional[str]:
        values = params.get(key)
        if not values:
            return None
        return values[0]

    @staticmethod
    def _is_https(params: Dict[str, list[str]]) -> bool:
        flag = TrinoClient._single_param(params, "https") or TrinoClient._single_param(params, "ssl") or TrinoClient._single_param(params, "tls") or TrinoClient._single_param(params, "httpScheme")
        if not flag:
            return False
        return flag.lower() in {"1", "true", "yes", "https"}

    @staticmethod
    def _parse_session_properties(params: Dict[str, list[str]]) -> Dict[str, str]:
        raw = TrinoClient._single_param(params, "sessionProperties")
        if not raw:
            return {}
        props: Dict[str, str] = {}
        for part in raw.split(","):
            if "=" not in part:
                continue
            key, value = part.split("=", 1)
            props[key.strip()] = value.strip()
        return props
