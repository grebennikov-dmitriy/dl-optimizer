import os
from celery import Celery

celery_app = Celery(
    "dlopt",
    broker=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
    backend=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
)

@celery_app.task(name="run_analysis")
def run_analysis(payload: dict) -> dict:
    from app.models import NewRequest
    from app.services.analyzer import Analyzer
    req = NewRequest(**payload)
    analyzer = Analyzer(req)
    return analyzer.run()
