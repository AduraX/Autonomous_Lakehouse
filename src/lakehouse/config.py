"""Application configuration via Pydantic Settings.

All config is loaded from environment variables (or .env file).
Validates at startup — if required values are missing, the app won't start.
"""

from enum import StrEnum
from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Environment(StrEnum):
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"


class PlatformType(StrEnum):
    MOCK = "mock"
    ADF = "adf"
    FABRIC = "fabric"
    DATABRICKS = "databricks"
    KUBEFLOW = "kubeflow"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # Application
    app_env: Environment = Environment.DEVELOPMENT
    app_debug: bool = False
    app_log_level: str = "INFO"

    # Database
    database_url: str = Field(
        default="postgresql+asyncpg://lakehouse:lakehouse_dev@localhost:5432/lakehouse",
        description="Async PostgreSQL connection string",
    )

    # Redis
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        description="Redis connection string for task queue",
    )

    # Observability
    otel_service_name: str = "autonomous-lakehouse"
    otel_exporter_otlp_endpoint: str = "http://localhost:4317"

    # Platform adapter
    platform_adapter: PlatformType = PlatformType.MOCK

    # Kubeflow
    kubeflow_host: str = ""
    kubeflow_namespace: str = "kubeflow-user"
    kubeflow_sa_token: str = ""

    # Iceberg (for Data Quality Agent)
    iceberg_rest_catalog_url: str = ""
    iceberg_warehouse: str = "s3://iceberg-warehouse"
    iceberg_s3_endpoint: str = ""
    iceberg_s3_access_key: str = ""
    iceberg_s3_secret_key: str = ""

    # Azure Data Factory
    adf_subscription_id: str = ""
    adf_resource_group: str = ""
    adf_factory_name: str = ""
    adf_tenant_id: str = ""
    adf_client_id: str = ""
    adf_client_secret: str = ""

    # Databricks
    databricks_workspace_url: str = ""
    databricks_token: str = ""

    # Microsoft Fabric
    fabric_workspace_id: str = ""
    fabric_tenant_id: str = ""
    fabric_client_id: str = ""
    fabric_client_secret: str = ""

    # Keycloak (for API authentication)
    keycloak_url: str = ""
    keycloak_realm: str = "kubeflow"
    keycloak_client_id: str = "lakehouse-ops"
    keycloak_client_secret: str = ""

    # LLM Integration
    llm_enabled: bool = False
    llm_provider: str = "claude"  # "claude", "local", "none"
    llm_model: str = "claude-sonnet-4-20250514"
    llm_confidence_threshold: float = 0.7  # Below this, escalate to LLM
    anthropic_api_key: str = ""

    # Local LLM (Ollama, vLLM, llama.cpp)
    llm_local_url: str = "http://localhost:11434/v1"
    llm_local_model: str = "llama3:8b"
    llm_local_api_key: str = ""

    @property
    def is_development(self) -> bool:
        return self.app_env == Environment.DEVELOPMENT

    @property
    def is_production(self) -> bool:
        return self.app_env == Environment.PRODUCTION

    @property
    def sync_database_url(self) -> str:
        """Alembic requires a synchronous URL."""
        return self.database_url.replace("asyncpg", "psycopg2")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Cached settings singleton — loaded once at startup."""
    return Settings()
