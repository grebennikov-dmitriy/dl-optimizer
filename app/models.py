from pydantic import BaseModel, Field
from typing import List, Optional

class DDLItem(BaseModel):
    statement: str

class QueryItem(BaseModel):
    queryid: str
    query: str
    runquantity: int = Field(ge=0)

class NewRequest(BaseModel):
    url: str  # JDBC
    ddl: List[DDLItem]
    queries: List[QueryItem]

class TaskResponse(BaseModel):
    taskid: str

class StatusResponse(BaseModel):
    status: str  # RUNNING | DONE | FAILED

class SQLStatement(BaseModel):
    statement: str

class QueryOut(BaseModel):
    queryid: str
    query: str

class ResultResponse(BaseModel):
    ddl: List[SQLStatement]
    migrations: List[SQLStatement]
    queries: List[QueryOut]
