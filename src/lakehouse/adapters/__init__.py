"""Platform adapters for lakehouse integrations."""

from lakehouse.adapters.base import PlatformAdapter
from lakehouse.config import PlatformType, get_settings


def get_adapter() -> PlatformAdapter:
    """Return the platform adapter based on current configuration."""
    settings = get_settings()

    match settings.platform_adapter:
        case PlatformType.ADF:
            from lakehouse.adapters.adf import AzureDataFactoryAdapter

            return AzureDataFactoryAdapter()
        case PlatformType.FABRIC:
            from lakehouse.adapters.fabric import FabricAdapter

            return FabricAdapter()
        case PlatformType.KUBEFLOW:
            from lakehouse.adapters.kubeflow import KubeflowAdapter

            return KubeflowAdapter()
        case PlatformType.DATABRICKS:
            from lakehouse.adapters.databricks import DatabricksAdapter

            return DatabricksAdapter()
        case _:
            from lakehouse.adapters.mock import MockAdapter

            return MockAdapter()
