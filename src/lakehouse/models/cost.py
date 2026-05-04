"""Cost baseline model — tracks historical duration and cost metrics per pipeline.

Stores rolling statistics computed from PipelineEvent history.
The Cost Optimization Agent uses these baselines to detect anomalies.
"""

from datetime import datetime

from sqlalchemy import DateTime, Index, String
from sqlalchemy.orm import Mapped, mapped_column

from lakehouse.models.base import Base


class CostBaseline(Base):
    """Rolling cost/duration baseline for a specific pipeline.

    Updated after each completed pipeline run. Stores enough statistics
    to detect anomalies without scanning full event history.
    """

    __tablename__ = "cost_baselines"

    # Identity — one row per pipeline
    pipeline_name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        unique=True,
        index=True,
    )

    # Duration statistics (seconds)
    avg_duration: Mapped[float] = mapped_column(nullable=False, insert_default=0.0)
    min_duration: Mapped[float] = mapped_column(nullable=False, insert_default=0.0)
    max_duration: Mapped[float] = mapped_column(nullable=False, insert_default=0.0)
    stddev_duration: Mapped[float] = mapped_column(
        nullable=False,
        insert_default=0.0,
        comment="Standard deviation of duration — used for z-score anomaly detection",
    )

    # Counters
    total_runs: Mapped[int] = mapped_column(nullable=False, insert_default=0)
    total_failures: Mapped[int] = mapped_column(nullable=False, insert_default=0)
    total_successes: Mapped[int] = mapped_column(nullable=False, insert_default=0)

    # Cost tracking (cumulative)
    total_duration_seconds: Mapped[float] = mapped_column(
        nullable=False,
        insert_default=0.0,
        comment="Sum of all run durations — proxy for compute cost",
    )

    # Last observation
    last_duration: Mapped[float | None] = mapped_column(nullable=True)
    last_run_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # Anomaly tracking
    consecutive_anomalies: Mapped[int] = mapped_column(
        nullable=False,
        insert_default=0,
        comment="Count of consecutive anomalous runs — resets on normal run",
    )

    __table_args__ = (Index("ix_baselines_last_run", "last_run_at"),)

    @property
    def failure_rate(self) -> float:
        """Fraction of runs that failed."""
        if self.total_runs == 0:
            return 0.0
        return self.total_failures / self.total_runs

    @property
    def avg_cost_per_run(self) -> float:
        """Average duration per run (proxy for cost)."""
        if self.total_runs == 0:
            return 0.0
        return self.total_duration_seconds / self.total_runs

    def __repr__(self) -> str:
        return (
            f"<CostBaseline(pipeline={self.pipeline_name}, "
            f"avg={self.avg_duration:.1f}s, runs={self.total_runs})>"
        )
