"""Helpers for extracting structural information from CREATE TABLE statements."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional

import sqlparse

from ..models import DDLItem


@dataclass
class TableDefinition:
    """Structured information extracted from a CREATE TABLE statement."""

    catalog: str
    schema: str
    table: str
    body: str  # everything after the table name (columns, WITH, etc.)
    original_statement: str


class DDLTools:
    _create_table_regex = re.compile(r"CREATE\s+TABLE\s+([`\"\w\.]+)", re.IGNORECASE)

    @staticmethod
    def catalog_of_first(ddl: List[DDLItem]) -> str:
        tables = DDLTools.parse_tables(ddl)
        if tables:
            return tables[0].catalog
        return "catalog"

    @staticmethod
    def parse_tables(ddl: List[DDLItem]) -> List[TableDefinition]:
        tables: List[TableDefinition] = []
        for item in ddl:
            parsed = DDLTools._parse_create_table(item.statement)
            if parsed:
                tables.append(parsed)
        return tables

    @staticmethod
    def _parse_create_table(statement: str) -> Optional[TableDefinition]:
        stmt = statement.strip().rstrip(";")
        match = DDLTools._create_table_regex.search(stmt)
        if not match:
            return None

        full_name = match.group(1).strip("`\"")
        parts = [p for p in full_name.split(".") if p]
        if len(parts) == 3:
            catalog, schema, table = parts
        elif len(parts) == 2:
            catalog = "catalog"
            schema, table = parts
        elif len(parts) == 1:
            catalog, schema = "catalog", "public"
            table = parts[0]
        else:
            return None

        body = stmt[match.end():].lstrip()
        if not body:
            # Attempt to recover the columns with sqlparse for unusual formatting.
            parsed = sqlparse.parse(stmt)
            if parsed:
                tokens = [t for t in parsed[0].tokens if t.ttype is None]
                if tokens and not tokens[-1].value.upper().startswith("CREATE TABLE"):
                    body = tokens[-1].value

        return TableDefinition(
            catalog=catalog,
            schema=schema,
            table=table,
            body=body if body else "",
            original_statement=stmt,
        )

