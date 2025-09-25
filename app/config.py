from pydantic import BaseModel
import os

class Settings(BaseModel):
    api_token: str = os.getenv("API_TOKEN", "change-me")
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    llm_provider: str = os.getenv("LLM_PROVIDER", "ollama")
    openai_api_key: str | None = os.getenv("OPENAI_API_KEY")
    openai_model: str = os.getenv("OPENAI_MODEL", "qwen2:7b")
    max_status_longpoll_seconds: int = int(os.getenv("MAX_STATUS_LONGPOLL_SECONDS", 1200))
    max_service_wait_minutes: int = int(os.getenv("MAX_SERVICE_WAIT_MINUTES", 15))

settings = Settings()
