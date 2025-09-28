import json
import re
import textwrap
import uuid
from typing import Dict, List, Optional
from ..models import NewRequest, SQLStatement, QueryOut
from ..utils.ddl_parser import DDLTools, TableDefinition
from ..utils.iceberg import recommend_table_properties
from ..utils.sql_rewriter import Rewriter
from .trino_client import TrinoClient
from .llm import LLM

class Analyzer:
    def __init__(self, req: NewRequest):
        self.req = req
        self.tables: List[TableDefinition] = DDLTools.parse_tables(req.ddl)
        self.catalog = self.tables[0].catalog if self.tables else DDLTools.catalog_of_first(req.ddl)
        self.new_schema = f"opt_{uuid.uuid4().hex[:8]}"
        self.trino = TrinoClient(req.url)
        self.llm = LLM()

    def run(self) -> dict:
        fallback_sections = {
            "ddl": self._ddl_section(),
            "migrations": self._migrations_section(),
            "queries": self._queries_section(),
        }

        llm_plan = self._llm_plan()
        if llm_plan:
            merged = self._merge_with_fallback(llm_plan, fallback_sections)
            if merged:
                return merged

        return self._sections_to_dict(fallback_sections)

    def _ddl_section(self) -> List[SQLStatement]:
        if self.tables:
            return self._ddl_from_existing_tables()
        props = recommend_table_properties()
        props_str = ",\n  ".join([f"'{k}'='{v}'" for k, v in props.items()])
        statements = [
            SQLStatement(statement=f"CREATE SCHEMA {self.catalog}.{self.new_schema}"),
            SQLStatement(statement=(
                f"CREATE TABLE {self.catalog}.{self.new_schema}.fact_events (\n"
                f"  event_id BIGINT,\n  user_id BIGINT,\n  ts TIMESTAMP,\n  sku VARCHAR,\n  price DOUBLE\n)\n"
                f"WITH (\n  partitioning = ARRAY['days(ts)'],\n  {props_str}\n)"
            )),
            SQLStatement(statement=(
                f"CREATE TABLE {self.catalog}.{self.new_schema}.dim_users (\n"
                f"  user_id BIGINT,\n  country VARCHAR,\n  segment VARCHAR\n)\n"
                f"WITH (\n  {props_str}\n)"
            )),
        ]
        return statements

    def _migrations_section(self) -> List[SQLStatement]:
        if self.tables:
            return self._migration_from_existing_tables()
        mig = [
            SQLStatement(statement=(
                f"INSERT INTO {self.catalog}.{self.new_schema}.fact_events\n"
                f"SELECT e.event_id, e.user_id, e.ts, e.sku, e.price\n"
                f"FROM {self.catalog}.public.events e LEFT JOIN {self.catalog}.public.items i\n"
                f"ON e.sku = i.sku"
            )),
            SQLStatement(statement=(
                f"INSERT INTO {self.catalog}.{self.new_schema}.dim_users\n"
                f"SELECT DISTINCT u.user_id, u.country, u.segment\n"
                f"FROM {self.catalog}.public.users u"
            )),
        ]
        return mig

    def _queries_section(self):
        mapping = self._table_mapping()
        return Rewriter.rewrite(self.req.queries, mapping)

    def _sections_to_dict(self, sections: Dict[str, List]) -> dict:
        return {
            "ddl": [s.model_dump() for s in sections["ddl"]],
            "migrations": [s.model_dump() for s in sections["migrations"]],
            "queries": [q.model_dump() for q in sections["queries"]],
        }

    def _ddl_from_existing_tables(self) -> List[SQLStatement]:
        statements: List[SQLStatement] = [
            SQLStatement(statement=f"CREATE SCHEMA {self.catalog}.{self.new_schema}"),
        ]
        props = recommend_table_properties()
        props_str = ",\n  ".join([f"'{k}'='{v}'" for k, v in props.items()])

        for table in self.tables:
            columns_and_options = table.body or ""
            statement = (
                f"CREATE TABLE {self.catalog}.{self.new_schema}.{table.table} "
                f"{columns_and_options}"
            ).rstrip()
            if "WITH" not in columns_and_options.upper():
                statement = (
                    f"{statement}\nWITH (\n  {props_str}\n)"
                )
            statements.append(SQLStatement(statement=statement))
        return statements

    def _migration_from_existing_tables(self) -> List[SQLStatement]:
        migrations: List[SQLStatement] = []
        for table in self.tables:
            migrations.append(
                SQLStatement(
                    statement=(
                        f"INSERT INTO {self.catalog}.{self.new_schema}.{table.table}\n"
                        f"SELECT * FROM {table.catalog}.{table.schema}.{table.table}"
                    )
                )
            )
        return migrations

    def _table_mapping(self) -> Dict[str, str]:
        mapping: Dict[str, str] = {}
        for table in self.tables:
            target = f"{self.catalog}.{self.new_schema}.{table.table}"
            source_variants = [
                f"{table.catalog}.{table.schema}.{table.table}",
                f'"{table.catalog}"."{table.schema}"."{table.table}"',
                f"{table.schema}.{table.table}",
                f'"{table.schema}"."{table.table}"',
                table.table,
                f'"{table.table}"',
            ]
            for variant in source_variants:
                if variant not in mapping:
                    mapping[variant] = target
        return mapping

    def _llm_plan(self) -> Optional[dict]:
        prompt = self._build_prompt()
        try:
            raw = self.llm.suggest(prompt)
        except Exception:
            return None

        snippet = self._extract_json_snippet(raw)
        if not snippet:
            return None
        try:
            return json.loads(snippet)
        except json.JSONDecodeError:
            return None

    def _merge_with_fallback(self, plan: dict, fallback: Dict[str, List]) -> Optional[dict]:
        ddl = self._select_ddl(plan.get("ddl"), fallback["ddl"])
        migrations = self._select_migrations(plan.get("migrations"), fallback["migrations"])
        queries = self._select_queries(plan.get("queries"), fallback["queries"])

        if ddl and migrations and queries:
            return self._sections_to_dict({"ddl": ddl, "migrations": migrations, "queries": queries})
        return None

    def _select_ddl(self, candidate, fallback: List[SQLStatement]) -> List[SQLStatement]:
        statements = self._to_sql_statements(candidate)
        if statements:
            statements = self._ensure_schema_statement(statements)
            if all(self._contains_new_schema(stmt.statement) for stmt in statements[1:]):
                return statements
        return fallback

    def _select_migrations(self, candidate, fallback: List[SQLStatement]) -> List[SQLStatement]:
        statements = self._to_sql_statements(candidate)
        if statements and all(self._contains_new_schema(stmt.statement) for stmt in statements):
            return statements
        return fallback

    def _select_queries(self, candidate, fallback: List[QueryOut]) -> List[QueryOut]:
        candidates = {q.queryid: q for q in self._to_query_outputs(candidate)}

        mapping = self._table_mapping()
        selected: List[QueryOut] = []
        for query in fallback:
            if query.queryid in candidates:
                text = candidates[query.queryid].query.strip()
                text = Rewriter.qualify_names(text, mapping)
                text = Rewriter.apply_rules(text)
                selected.append(QueryOut(queryid=query.queryid, query=text))
            else:
                selected.append(query)

        prefix = f"{self.catalog}.{self.new_schema}.".lower()
        if selected and all(prefix in q.query.lower() for q in selected):
            return selected
        return fallback

    def _to_sql_statements(self, items) -> List[SQLStatement]:
        statements: List[SQLStatement] = []
        if not isinstance(items, list):
            return statements
        for item in items:
            if isinstance(item, dict):
                statement = item.get("statement") or item.get("sql")
            else:
                statement = str(item)
            if not statement:
                continue
            normalized = self._normalize_statement(statement)
            if normalized:
                statements.append(SQLStatement(statement=normalized))
        return statements

    def _to_query_outputs(self, items) -> List[QueryOut]:
        queries: List[QueryOut] = []
        if not isinstance(items, list):
            return queries
        for item in items:
            if isinstance(item, dict):
                queryid = item.get("queryid") or item.get("query_id")
                query = item.get("query") or item.get("sql")
            elif isinstance(item, (list, tuple)) and len(item) >= 2:
                queryid, query = item[0], item[1]
            else:
                continue
            if not queryid or not query:
                continue
            queries.append(QueryOut(queryid=str(queryid), query=str(query)))
        return queries

    def _ensure_schema_statement(self, statements: List[SQLStatement]) -> List[SQLStatement]:
        required = f"CREATE SCHEMA {self.catalog}.{self.new_schema}"
        cleaned: List[SQLStatement] = []
        seen = set()
        for stmt in statements:
            normalized = self._normalize_statement(stmt.statement)
            if not normalized:
                continue
            key = normalized.lower()
            if key in seen:
                continue
            seen.add(key)
            cleaned.append(SQLStatement(statement=normalized))

        if cleaned and cleaned[0].statement.lower().startswith("create schema"):
            cleaned[0] = SQLStatement(statement=required)
        else:
            cleaned.insert(0, SQLStatement(statement=required))
        return cleaned

    def _normalize_statement(self, statement: str) -> str:
        return statement.strip().rstrip(";")

    def _contains_new_schema(self, statement: str) -> bool:
        prefix = f"{self.catalog}.{self.new_schema}.".lower()
        return prefix in statement.lower()

    def _build_prompt(self) -> str:
        ddl_lines = []
        for table in self.tables:
            summary = table.body.replace("\n", " ") if table.body else "(columns unavailable)"
            ddl_lines.append(f"- {table.catalog}.{table.schema}.{table.table}: {summary[:280]}")
        if not ddl_lines:
            ddl_lines.append("- (no existing tables parsed; design a star-schema around events and dimensions)")

        query_lines = []
        for q in self.req.queries:
            snippet = q.query.replace("\n", " ")
            query_lines.append(f"- {q.queryid} (runs {q.runquantity}): {snippet[:320]}")
        if not query_lines:
            query_lines.append("- (no queries provided)")

        prompt = f"""
        You are an expert in Trino + Apache Iceberg performance optimisation.
        Catalogue name: {self.catalog}
        Use the new schema name {self.new_schema} for all optimised artefacts.

        Existing tables:
        {chr(10).join(ddl_lines)}

        Observed SQL workload:
        {chr(10).join(query_lines)}

        Produce a JSON object with keys "ddl", "migrations" and "queries".
        Each list item must be an object with a "statement" (for ddl/migrations) or "query" and "queryid" (for queries).
        Rules:
        1. Fully qualify every table reference as {self.catalog}.<schema>.<table>.
        2. The first DDL statement must be exactly "CREATE SCHEMA {self.catalog}.{self.new_schema}".
        3. Rewritten queries must keep their original queryid values and target the new schema {self.new_schema}.
        4. Optimise for denormalised or star-schema patterns and Iceberg best practices (partitioning, properties).
        5. Respond with JSON only (no Markdown fences, no explanations).
        """
        return textwrap.dedent(prompt).strip()

    @staticmethod
    def _extract_json_snippet(text: str) -> Optional[str]:
        if not text:
            return None
        fenced = re.search(r"```(?:json)?\s*(\{.*?\})```", text, re.DOTALL)
        if fenced:
            return fenced.group(1)
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            return None
        return text[start : end + 1]
