"""Shared fixtures and configuration for integration tests.

These tests hit real cloud services and require valid credentials
in environment variables. Tests are automatically skipped when the
required credentials are not present.

Usage:
    pytest tests/integration/ --platform adf
    pytest tests/integration/ -k fabric
"""

import os

import pytest

from lakehouse.config import get_settings

# ---------------------------------------------------------------------------
# pytest CLI option
# ---------------------------------------------------------------------------


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add --platform CLI option for filtering integration tests."""
    parser.addoption(
        "--platform",
        action="store",
        default=None,
        help="Run integration tests only for this platform (adf, fabric, databricks, kubeflow)",
    )


@pytest.fixture
def platform(request: pytest.FixtureRequest) -> str | None:
    """Return the --platform CLI value (or None if not provided)."""
    return request.config.getoption("--platform")


# ---------------------------------------------------------------------------
# Skip decorators — check for required env vars
# ---------------------------------------------------------------------------

_ADF_ENV_VARS = (
    "ADF_SUBSCRIPTION_ID",
    "ADF_RESOURCE_GROUP",
    "ADF_FACTORY_NAME",
    "ADF_CLIENT_SECRET",
)

_FABRIC_ENV_VARS = (
    "FABRIC_WORKSPACE_ID",
    "FABRIC_CLIENT_ID",
    "FABRIC_CLIENT_SECRET",
)

_DATABRICKS_ENV_VARS = (
    "DATABRICKS_HOST",
    "DATABRICKS_TOKEN",
)

_KUBEFLOW_ENV_VARS = (
    "KUBEFLOW_HOST",
    "KUBEFLOW_SA_TOKEN",
)


def _missing_vars(var_names: tuple[str, ...]) -> list[str]:
    return [v for v in var_names if not os.environ.get(v)]


requires_adf = pytest.mark.skipif(
    bool(_missing_vars(_ADF_ENV_VARS)),
    reason=f"Missing ADF env vars: {_missing_vars(_ADF_ENV_VARS)}",
)

requires_fabric = pytest.mark.skipif(
    bool(_missing_vars(_FABRIC_ENV_VARS)),
    reason=f"Missing Fabric env vars: {_missing_vars(_FABRIC_ENV_VARS)}",
)

requires_databricks = pytest.mark.skipif(
    bool(_missing_vars(_DATABRICKS_ENV_VARS)),
    reason=f"Missing Databricks env vars: {_missing_vars(_DATABRICKS_ENV_VARS)}",
)

requires_kubeflow = pytest.mark.skipif(
    bool(_missing_vars(_KUBEFLOW_ENV_VARS)),
    reason=f"Missing Kubeflow env vars: {_missing_vars(_KUBEFLOW_ENV_VARS)}",
)

# ---------------------------------------------------------------------------
# Adapter fixtures — create real instances from environment
# ---------------------------------------------------------------------------


@pytest.fixture
def adf_adapter():
    """Create a real AzureDataFactoryAdapter from environment variables."""
    get_settings.cache_clear()
    from lakehouse.adapters.adf import AzureDataFactoryAdapter

    adapter = AzureDataFactoryAdapter()
    yield adapter
    get_settings.cache_clear()


@pytest.fixture
def fabric_adapter():
    """Create a real FabricAdapter from environment variables."""
    get_settings.cache_clear()
    from lakehouse.adapters.fabric import FabricAdapter

    adapter = FabricAdapter()
    yield adapter
    get_settings.cache_clear()


@pytest.fixture
def databricks_adapter():
    """Create a real DatabricksAdapter from environment variables.

    Note: DatabricksAdapter is not yet implemented. This fixture is a
    placeholder that will work once the adapter module exists.
    """
    get_settings.cache_clear()
    try:
        from lakehouse.adapters.databricks import DatabricksAdapter

        adapter = DatabricksAdapter()
        yield adapter
    except ImportError:
        pytest.skip("DatabricksAdapter not yet implemented")
    finally:
        get_settings.cache_clear()


@pytest.fixture
def kubeflow_adapter():
    """Create a real KubeflowAdapter from environment variables."""
    get_settings.cache_clear()
    from lakehouse.adapters.kubeflow import KubeflowAdapter

    adapter = KubeflowAdapter()
    yield adapter
    get_settings.cache_clear()
