from ..config import settings
import httpx
from functools import lru_cache

# Pluggable LLM abstraction supporting local Ollama (Qwen3 14B) by default.
# Set LLM_PROVIDER=ollama and ensure an Ollama daemon exposes the qwen3:14b model
# (e.g., by running `ollama pull qwen3:14b`).

class LLM:
    def __init__(self):
        self.provider = settings.llm_provider.lower()
        self.model_name = settings.openai_model  # reused as generic model name
        self.ollama_base = getenv_default("OLLAMA_BASE_URL", "http://host.docker.internal:11434")
        self.openai_api_key = settings.openai_api_key
        self.qwen_model_path = settings.qwen_model_path
        self.qwen_device = settings.qwen_device
        self.qwen_dtype = settings.qwen_dtype
        self.qwen_max_new_tokens = settings.qwen_max_new_tokens
        self.qwen_temperature = settings.qwen_temperature

    def suggest(self, prompt: str) -> str:
        if self.provider in {"qwen", "qwen_local", "transformers"}:
            return self._qwen_local_chat(prompt)
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
            "model": self.model_name or "qwen3:14b",
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
                model=self.model_name or "gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a SQL & Iceberg performance expert."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2,
            )
            return resp.choices[0].message.content
        except Exception as e:
            return f"-- OpenAI error: {e}\n{prompt[:4000]}"

    def _qwen_local_chat(self, prompt: str) -> str:
        """Run inference with a locally available Qwen model via transformers."""
        try:
            tokenizer, model, device = _load_qwen_model(
                self.qwen_model_path,
                self.qwen_device,
                self.qwen_dtype,
            )
        except Exception as exc:
            return f"-- Qwen load error: {exc}\n{prompt[:4000]}"

        try:
            import torch

            inputs = tokenizer(prompt, return_tensors="pt")
            inputs = {k: v.to(device) for k, v in inputs.items()}

            eos_token_id = tokenizer.eos_token_id
            if eos_token_id is None and tokenizer.eos_token:
                eos_token_id = tokenizer.convert_tokens_to_ids(tokenizer.eos_token)
            pad_token_id = tokenizer.pad_token_id or eos_token_id
            if pad_token_id is None:
                pad_token_id = 0

            generation_kwargs = {
                "max_new_tokens": self.qwen_max_new_tokens,
                "temperature": self.qwen_temperature,
                "do_sample": False,
                "eos_token_id": eos_token_id,
                "pad_token_id": pad_token_id,
            }
            with torch.no_grad():
                output_ids = model.generate(**inputs, **generation_kwargs)

            generated = output_ids[0, inputs["input_ids"].shape[1]:]
            return tokenizer.decode(generated, skip_special_tokens=True).strip()
        except Exception as exc:
            return f"-- Qwen inference error: {exc}\n{prompt[:4000]}"


def getenv_default(key: str, default: str) -> str:
    import os
    v = os.getenv(key)
    return v if v else default


@lru_cache(maxsize=2)
def _load_qwen_model(model_path: str, device: str, dtype: str):
    """Load a Qwen model once and cache it for reuse."""
    try:
        from transformers import AutoModelForCausalLM, AutoTokenizer
        import torch
    except ImportError as exc:
        raise RuntimeError("transformers/torch are required for qwen_local provider") from exc

    tokenizer = AutoTokenizer.from_pretrained(model_path, trust_remote_code=True)
    if tokenizer.pad_token_id is None and tokenizer.eos_token_id is not None:
        tokenizer.pad_token_id = tokenizer.eos_token_id

    torch_dtype = None
    if dtype and dtype.lower() != "auto":
        try:
            torch_dtype = getattr(torch, dtype.lower())
        except AttributeError as exc:
            raise RuntimeError(f"Unsupported torch dtype: {dtype}") from exc

    model = AutoModelForCausalLM.from_pretrained(
        model_path,
        trust_remote_code=True,
        torch_dtype=torch_dtype,
    )

    resolved_device = device or "cpu"
    if resolved_device == "auto":
        resolved_device = "cuda" if torch.cuda.is_available() else "cpu"

    model.to(resolved_device)
    model.eval()

    return tokenizer, model, resolved_device
