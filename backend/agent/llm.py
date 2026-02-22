"""LLM factory — returns ollama or openai-based chat model based on config."""

from functools import lru_cache

from langchain_core.language_models import BaseChatModel

from backend.config import get_settings


@lru_cache(maxsize=1)
def get_llm() -> BaseChatModel:
    settings = get_settings()

    if settings.llm_provider == "ollama":
        from langchain_community.chat_models import ChatOllama
        return ChatOllama(
            base_url=settings.ollama_base_url,
            model=settings.ollama_model,
            temperature=0,
        )
    elif settings.llm_provider == "openai":
        from langchain_openai import ChatOpenAI
        return ChatOpenAI(
            api_key=settings.openai_api_key,
            model=settings.openai_model,
            temperature=0,
        )
    else:
        raise ValueError(f"Unknown LLM provider: {settings.llm_provider}")
