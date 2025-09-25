from fastapi import FastAPI
from .routers import tasks

app = FastAPI(title="DL Optimizer API", version="1.0.0")
app.include_router(tasks.router)
