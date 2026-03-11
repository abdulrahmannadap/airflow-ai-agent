"""LLM Factory — connects to Ollama, gracefully handles offline."""
from loguru import logger
from config.settings import get_settings

settings = get_settings()

def get_llm(fast: bool = False):
    try:
        import httpx
        from langchain_ollama import OllamaLLM
        model = settings.FAST_LLM_MODEL if fast else settings.LLM_MODEL
        resp = httpx.get(f"{settings.OLLAMA_BASE_URL}/api/tags", timeout=3)
        if resp.status_code != 200:
            return None
        return OllamaLLM(
            model=model,
            base_url=settings.OLLAMA_BASE_URL,
            temperature=0.0,
            num_ctx=4096,
            num_predict=1024,
        )
    except Exception as e:
        logger.warning(f"Ollama offline: {e} — using rule-based mode")
        return None

def check_ollama_status() -> dict:
    try:
        import httpx
        resp = httpx.get(f"{settings.OLLAMA_BASE_URL}/api/tags", timeout=3)
        if resp.status_code == 200:
            models = [m["name"] for m in resp.json().get("models", [])]
            return {"status": "online", "models": models}
    except Exception:
        pass
    return {"status": "offline", "models": []}
