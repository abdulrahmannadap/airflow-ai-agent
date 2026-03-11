from pydantic_settings import BaseSettings
from functools import lru_cache

class Settings(BaseSettings):
    APP_NAME: str = "Airflow AI Agent"
    APP_ENV: str = "development"
    DEBUG: bool = True

    # LLM
    OLLAMA_BASE_URL: str = "http://localhost:11434"
    LLM_MODEL: str = "llama3.1:8b-instruct-q4_K_M"
    FAST_LLM_MODEL: str = "phi3:mini"

    # Airflow
    AIRFLOW_BASE_URL: str = "http://localhost:8080"
    AIRFLOW_USERNAME: str = "admin"
    AIRFLOW_PASSWORD: str = "admin"
    AIRFLOW_LOG_DIR: str = "./data/logs/airflow"

    # Database
    DATABASE_URL: str = "sqlite:///./airflow_agent.db"

    # Agent behaviour
    MONITOR_INTERVAL_SECONDS: int = 30
    DEPENDENCY_CHECK_INTERVAL_SECONDS: int = 60
    MAX_RETRIES: int = 5
    MAX_WAIT_MINUTES: int = 120

    # Resource thresholds
    CPU_THRESHOLD_PERCENT: float = 80.0
    MEMORY_THRESHOLD_PERCENT: float = 85.0

    class Config:
        env_file = ".env"

@lru_cache()
def get_settings() -> Settings:
    return Settings()
