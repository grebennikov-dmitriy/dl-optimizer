import sqlparse
from typing import List
from ..models import DDLItem

class DDLTools:
    @staticmethod
    def catalog_of_first(ddl: List[DDLItem]) -> str:
        # Extract catalog from first CREATE TABLE catalog.schema.table
        for item in ddl:
            parsed = sqlparse.parse(item.statement)
            if not parsed:
                continue
            tokens = [t for t in parsed[0].tokens if t.ttype is None]
            for i, t in enumerate(tokens):
                if t.value.upper().startswith("CREATE TABLE"):
                    if i + 1 < len(tokens):
                        fullname = tokens[i+1].value.strip().strip("`")
                        parts = fullname.split(".")
                        if len(parts) >= 3:
                            return parts[0]
        return "catalog"
