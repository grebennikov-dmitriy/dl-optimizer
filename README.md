# DL Optimizer — Non-Visual REST Service (LLM-assisted)

Implements the exact REST API from the ТЗ: `/new`, `/status`, `/getresult`. Asynchronous analysis via Celery + Redis. Read-only analysis, returns DDL/MIGRATIONS/QUERIES sections with fully qualified names and first DDL command `CREATE SCHEMA <catalog>.<new_schema>`.

## Quickstart

```bash
cp .env.example .env
# Edit API_TOKEN and LLM settings (Ollama by default)
docker compose up --build -d
```

### Health / OpenAPI
- API: http://localhost:8080
- Auth: pass header `X-API-Token: <API_TOKEN>`

## Endpoints

### `POST /new`
```json
{
  "url": "jdbc://trino-host:8080/catalog?user=xxx&password=yyyy",
  "ddl": [ {"statement": "CREATE TABLE catalog.public.Table1 (...)"} ],
  "queries": [ {"queryid":"uuid","query":"SELECT ...","runquantity":123} ]
}
```
**Response** `{ "taskid": "<uuid>" }`

### `GET /status?task_id=<uuid>`
- Supports long-poll up to 20 minutes (configurable via `MAX_STATUS_LONGPOLL_SECONDS`).
- Returns: `RUNNING | DONE | FAILED`.

### `GET /getresult?task_id=<uuid>`
- Returns strict JSON with `ddl`, `migrations`, `queries`.

## VS Code Usage
1. Install extensions: **Docker**, **Python**, **REST Client** (optional), **Celery** (optional).
2. Open the folder in VS Code.
3. Copy `.env.example` → `.env`, set `API_TOKEN` and verify `OLLAMA_BASE_URL`.
4. Press `Ctrl+Shift+B` to run the **Docker: up** task, or use **Run and Debug** → **FastAPI (Docker Compose)** from the launch menu.
5. Use `requests.http` to call API directly from VS Code (REST Client), or use cURL commands below.

## Local (no Docker) Dev Option
Create a venv and run services directly (Ollama on localhost):
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export LLM_PROVIDER=ollama
export OPENAI_MODEL=qwen2:7b
export OLLAMA_BASE_URL=http://localhost:11434
export API_TOKEN=change-me
export REDIS_URL=redis://localhost:6379/0
# Terminal 1 (Celery)
celery -A worker.celery_app:celery_app worker --loglevel=INFO
# Terminal 2 (API)
uvicorn app.main:app --reload --port 8080
```

## Security
- Token auth via `X-API-Token` header.
- Credentials in JDBC string are not persisted beyond analysis task.

## Timeouts
- `/status` long-poll ≤ 20 min.
- Organizer overall wait ≤ 15 min (config).

## Testing (cURL)
```bash
TOKEN=change-me
BASE=http://localhost:8080

curl -s -H "X-API-Token: $TOKEN"   -H 'Content-Type: application/json'   -d '{
    "url":"jdbc://trino:8080/catalog?user=u&password=p",
    "ddl":[{"statement":"CREATE TABLE catalog.public.events (event_id bigint)"}],
    "queries":[{"queryid":"1","query":"SELECT * FROM catalog.public.events","runquantity":10}]
  }'   $BASE/new | jq

# Poll status (long-poll by default)
curl -s -H "X-API-Token: $TOKEN" "$BASE/status?task_id=<uuid>" | jq

# Get result once DONE
curl -s -H "X-API-Token: $TOKEN" "$BASE/getresult?task_id=<uuid>" | jq
```
