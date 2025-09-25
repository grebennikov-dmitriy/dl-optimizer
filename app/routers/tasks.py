import time
import uuid
from fastapi import APIRouter, Depends, HTTPException, Query
from ..auth import require_token
from ..models import NewRequest, TaskResponse, StatusResponse, ResultResponse
from ..storage.repo import Repo
from ..storage.schema import TaskRecord
from ..config import settings
from worker.celery_app import run_analysis

router = APIRouter()
repo = Repo()

@router.post("/new", response_model=TaskResponse)
async def new_task(payload: NewRequest, _=Depends(require_token)):
    taskid = str(uuid.uuid4())
    repo.save(TaskRecord(taskid=taskid, status="RUNNING"))
    async_result = run_analysis.delay(payload.model_dump())
    repo.r.hset(f"task:{taskid}", "celery_id", async_result.id)
    return TaskResponse(taskid=taskid)

@router.get("/status", response_model=StatusResponse)
async def get_status(task_id: str = Query(..., alias="task_id"), longpoll: bool = True, _=Depends(require_token)):
    rec = repo.get(task_id)
    if not rec:
        raise HTTPException(status_code=404, detail="Unknown task")

    deadline = time.time() + settings.max_status_longpoll_seconds
    while longpoll and rec.status == "RUNNING" and time.time() < deadline:
        time.sleep(1.0)
        celery_id = repo.r.hget(f"task:{task_id}", "celery_id")
        if celery_id:
            from celery.result import AsyncResult
            ar = AsyncResult(celery_id)
            if ar.failed():
                repo.set_status(task_id, "FAILED", error=str(ar.result))
            elif ar.successful():
                repo.set_status(task_id, "DONE")
                repo.set_result(task_id, ar.result)
        rec = repo.get(task_id)

    return StatusResponse(status=rec.status)

@router.get("/getresult", response_model=ResultResponse)
async def get_result(task_id: str = Query(..., alias="task_id"), _=Depends(require_token)):
    rec = repo.get(task_id)
    if not rec:
        raise HTTPException(status_code=404, detail="Unknown task")
    if rec.status != "DONE" or not rec.result_json:
        raise HTTPException(status_code=409, detail=f"Task status is {rec.status}")
    import orjson
    return ResultResponse(**orjson.loads(rec.result_json))
