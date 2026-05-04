"""Observability instrumentation — OpenTelemetry tracing + Prometheus metrics.

Provides:
  - OpenTelemetry tracer for distributed tracing across agents and adapters
  - Custom Prometheus counters, histograms, and gauges for business-level metrics
  - Convenience functions for instrumenting the FastAPI app

Tracing spans flow: API request → coordinator → adapter → executor
Metrics are scraped by Prometheus at /metrics (mounted in app.py).
"""

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from prometheus_client import Counter, Gauge, Histogram

from lakehouse.config import get_settings
from lakehouse.logging import get_logger

logger = get_logger(__name__)

# ============================================================
# OpenTelemetry Tracing
# ============================================================

_tracer_initialized = False


def setup_tracing() -> None:
    """Initialize OpenTelemetry tracing.

    Call once at application startup. Configures the tracer provider
    with a console exporter (dev) or OTLP exporter (production).
    """
    global _tracer_initialized
    if _tracer_initialized:
        return

    settings = get_settings()

    resource = Resource.create(
        {
            "service.name": settings.otel_service_name,
            "service.version": "0.1.0",
            "deployment.environment": settings.app_env.value,
        }
    )

    provider = TracerProvider(resource=resource)

    if settings.is_development:
        provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
    else:
        # In production, use OTLP exporter to send to a collector
        try:
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (  # type: ignore[import-not-found]
                OTLPSpanExporter,
            )

            otlp_exporter = OTLPSpanExporter(
                endpoint=settings.otel_exporter_otlp_endpoint,
            )
            provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
        except ImportError:
            logger.warning(
                "otlp_exporter_unavailable",
                msg="Install opentelemetry-exporter-otlp-proto-grpc for production tracing",
            )
            provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))

    trace.set_tracer_provider(provider)
    _tracer_initialized = True

    logger.info("tracing_initialized", service=settings.otel_service_name)


def get_tracer(name: str) -> trace.Tracer:
    """Get a named tracer for creating spans."""
    return trace.get_tracer(name)


# ============================================================
# Prometheus Metrics
# ============================================================

# --- Event Metrics ---
EVENTS_INGESTED = Counter(
    "lakehouse_events_ingested_total",
    "Total pipeline events ingested",
    ["platform", "status"],
)

# --- Coordination Metrics ---
COORDINATIONS_TOTAL = Counter(
    "lakehouse_coordinations_total",
    "Total coordination runs",
    ["category"],
)

COORDINATION_DURATION = Histogram(
    "lakehouse_coordination_duration_seconds",
    "Time spent coordinating an event",
    buckets=[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)

# --- Diagnosis Metrics ---
DIAGNOSES_CREATED = Counter(
    "lakehouse_diagnoses_created_total",
    "Total diagnoses created",
    ["category", "agent"],
)

# --- Action Metrics ---
ACTIONS_PROPOSED = Counter(
    "lakehouse_actions_proposed_total",
    "Total actions proposed",
    ["action_type", "policy"],
)

ACTIONS_EXECUTED = Counter(
    "lakehouse_actions_executed_total",
    "Total actions executed",
    ["action_type", "status"],
)

ACTION_EXECUTION_DURATION = Histogram(
    "lakehouse_action_execution_duration_seconds",
    "Time spent executing an action",
    ["action_type"],
    buckets=[0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0],
)

ACTION_RETRY_COUNT = Counter(
    "lakehouse_action_retries_total",
    "Total retry attempts for action execution",
    ["action_type"],
)

# --- Data Quality Metrics ---
DQ_CHECKS_TOTAL = Counter(
    "lakehouse_dq_checks_total",
    "Total data quality checks run",
    ["check_name", "status"],
)

DQ_SCAN_DURATION = Histogram(
    "lakehouse_dq_scan_duration_seconds",
    "Time spent scanning a namespace for quality",
    buckets=[1.0, 5.0, 10.0, 30.0, 60.0],
)

# --- Cost Metrics ---
COST_ANOMALIES_DETECTED = Counter(
    "lakehouse_cost_anomalies_total",
    "Total cost anomalies detected",
    ["anomaly_type", "severity"],
)

PIPELINE_DURATION = Histogram(
    "lakehouse_pipeline_duration_seconds",
    "Pipeline run durations",
    ["pipeline_name", "status"],
    buckets=[10, 30, 60, 120, 300, 600, 1200, 3600],
)

# --- System Metrics ---
ACTIVE_COORDINATIONS = Gauge(
    "lakehouse_active_coordinations",
    "Currently running coordination workflows",
)

ADAPTER_CALLS = Counter(
    "lakehouse_adapter_calls_total",
    "Total adapter API calls",
    ["adapter", "method", "status"],
)

ADAPTER_CALL_DURATION = Histogram(
    "lakehouse_adapter_call_duration_seconds",
    "Duration of adapter API calls",
    ["adapter", "method"],
    buckets=[0.1, 0.5, 1.0, 2.5, 5.0, 10.0],
)
