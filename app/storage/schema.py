from pydantic import BaseModel
from typing import Optional

class TaskRecord(BaseModel):
    taskid: str
    status: str  # RUNNING | DONE | FAILED
    error: Optional[str] = None
    result_json: Optional[str] = None  # orjson string
