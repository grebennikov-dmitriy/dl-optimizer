import uuid
from typing import Dict, List
from ..models import NewRequest, SQLStatement
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
