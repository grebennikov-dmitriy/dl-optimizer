import uuid
from typing import List
from ..models import NewRequest, SQLStatement
from ..utils.ddl_parser import DDLTools
from ..utils.iceberg import recommend_table_properties
from ..utils.sql_rewriter import Rewriter
from .trino_client import TrinoClient
from .llm import LLM

class Analyzer:
    def __init__(self, req: NewRequest):
        self.req = req
        self.catalog = DDLTools.catalog_of_first(req.ddl)
        self.new_schema = f"opt_{uuid.uuid4().hex[:8]}"
        self.trino = TrinoClient(req.url)
        self.llm = LLM()

    def _ddl_section(self) -> List[SQLStatement]:
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
        return Rewriter.rewrite(self.req.queries)

    def run(self) -> dict:
        prompt = (
            "Rewrite SQL for Trino + Iceberg with fully qualified names, ensure partition filters and avoid SELECT *.\n"
            "Also propose DDL star schema and data migrations."
        )
        _ = self.llm.suggest(prompt)  # result not directly used in minimal build
        return {
            "ddl": [s.model_dump() for s in self._ddl_section()],
            "migrations": [s.model_dump() for s in self._migrations_section()],
            "queries": [q.model_dump() for q in self._queries_section()],
        }
