import re
from typing import Dict, List
from ..models import QueryItem, QueryOut

RULES = [
    ("SELECT *", "SELECT /* explicit columns required */"),
    ("JOIN", "/* ensure join keys are partition/sort-aligned */ JOIN"),
]

class Rewriter:
    @staticmethod
    def qualify_names(q: str, table_mapping: Dict[str, str]) -> str:
        return Rewriter.replace_tables(q, table_mapping)

    @staticmethod
    def apply_rules(q: str) -> str:
        out = q
        for a, b in RULES:
            out = out.replace(a, b)
        return out

    @staticmethod
    def replace_tables(query: str, table_mapping: Dict[str, str]) -> str:
        result = query
        for source, target in table_mapping.items():
            if source.startswith(('"', "`")) and source.endswith(('"', "`")):
                pattern = re.compile(rf"(?i){re.escape(source)}")
            else:
                pattern = re.compile(rf"(?i)\b{re.escape(source)}\b")
            result = pattern.sub(target, result)
        return result

    @staticmethod
    def rewrite(queries: List[QueryItem], table_mapping: Dict[str, str]) -> List[QueryOut]:
        out: List[QueryOut] = []
        for qi in queries:
            q2 = Rewriter.qualify_names(qi.query, table_mapping)
            q3 = Rewriter.apply_rules(q2)
            out.append(QueryOut(queryid=qi.queryid, query=q3))
        return out
