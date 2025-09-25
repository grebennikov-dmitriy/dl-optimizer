from typing import List
from ..models import QueryItem

class Metrics:
    @staticmethod
    def weighted_runtime_baseline(queries: List[QueryItem]) -> int:
        # Placeholder: sum frequencies as proxy
        return sum(q.runquantity for q in queries)
