from typing import List
from ..models import QueryItem, QueryOut

RULES = [
    ("SELECT *", "SELECT /* explicit columns required */"),
    ("JOIN", "/* ensure join keys are partition/sort-aligned */ JOIN"),
]

class Rewriter:
    @staticmethod
    def qualify_names(q: str) -> str:
        # TODO: parse AST and qualify names properly
        return q

    @staticmethod
    def apply_rules(q: str) -> str:
        out = q
        for a, b in RULES:
            out = out.replace(a, b)
        return out

    @staticmethod
    def rewrite(queries: List[QueryItem]) -> List[QueryOut]:
        out: List[QueryOut] = []
        for qi in queries:
            q2 = Rewriter.qualify_names(qi.query)
            q3 = Rewriter.apply_rules(q2)
            out.append(QueryOut(queryid=qi.queryid, query=q3))
        return out
