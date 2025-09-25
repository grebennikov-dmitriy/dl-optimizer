import redis
from .schema import TaskRecord
from ..config import settings
import orjson

class Repo:
    def __init__(self):
        self.r = redis.from_url(settings.redis_url, decode_responses=True)

    def _key(self, taskid: str) -> str:
        return f"task:{taskid}"

    def save(self, rec: TaskRecord):
        self.r.hset(self._key(rec.taskid), mapping={
            "status": rec.status,
            "error": rec.error or "",
            "result_json": rec.result_json or "",
        })

    def get(self, taskid: str) -> TaskRecord | None:
        data = self.r.hgetall(self._key(taskid))
        if not data:
            return None
        return TaskRecord(taskid=taskid, status=data.get("status", "RUNNING"),
                          error=data.get("error") or None,
                          result_json=data.get("result_json") or None)

    def set_status(self, taskid: str, status: str, error: str | None = None):
        self.r.hset(self._key(taskid), mapping={"status": status, "error": error or ""})

    def set_result(self, taskid: str, result: dict):
        self.r.hset(self._key(taskid), "result_json", orjson.dumps(result).decode())
