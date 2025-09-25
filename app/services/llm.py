from ..config import settings
import httpx

# Pluggable LLM abstraction supporting local Ollama by default.
# Set LLM_PROVIDER=ollama and OLLAMA_BASE_URL (e.g., http://host.docker.internal:11434)

class LLM:
    def __init__(self):
        self.provider = settings.llm_provider.lower()
        self.model = settings.openai_model  # reused as generic model name
        self.ollama_base = getenv_default("OLLAMA_BASE_URL", "http://host.docker.internal:11434")
        self.openai_api_key = settings.openai_api_key

    def suggest(self, prompt: str) -> str:
        if self.provider == "ollama":
            return self._ollama_chat(prompt)
        elif self.provider == "openai" and self.openai_api_key:
            return self._openai_chat(prompt)
        # Deterministic fallback
        return f"-- LLM placeholder output based on prompt --\n{prompt[:4000]}"

    def _ollama_chat(self, prompt: str) -> str:
        # Uses Ollama /api/generate for a simple prompt → completion call
        url = f"{self.ollama_base.rstrip('/')}/api/generate"
        payload = {
            "model": self.model or "qwen2:7b",
            "prompt": prompt,
            "stream": False
        }
        try:
            with httpx.Client(timeout=120) as client:
                r = client.post(url, json=payload)
                r.raise_for_status()
                data = r.json()
                return data.get("response", "")
        except Exception as e:
            return f"-- Ollama error: {e}\n{prompt[:4000]}"

    def _openai_chat(self, prompt: str) -> str:
        try:
            from openai import OpenAI
            client = OpenAI()
            resp = client.chat.completions.create(
                model=self.model or "gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a SQL & Iceberg performance expert."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2,
            )
            return resp.choices[0].message.content
        except Exception as e:
            return f"-- OpenAI error: {e}\n{prompt[:4000]}"


def getenv_default(key: str, default: str) -> str:
    import os
    v = os.getenv(key)
    return v if v else default
