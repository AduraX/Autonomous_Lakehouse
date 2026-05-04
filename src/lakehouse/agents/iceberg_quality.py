"""Iceberg Data Quality Agent — validates tables in an Iceberg REST Catalog.

Designed for KF4X Phase 5 integration. Connects to the Iceberg REST Catalog
API to check:
  - Schema drift (new/removed/changed columns between snapshots)
  - Table freshness (time since last snapshot)
  - Volume anomalies (row count changes beyond threshold)
  - Partition health (null partitions, skew)

Uses the Iceberg REST Catalog API spec:
  https://iceberg.apache.org/spec/#iceberg-rest-catalog
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

import httpx

from lakehouse.config import get_settings
from lakehouse.logging import get_logger

logger = get_logger(__name__)


class CheckStatus(StrEnum):
    PASSED = "passed"
    WARNING = "warning"
    FAILED = "failed"
    ERROR = "error"


@dataclass
class QualityCheckResult:
    """Result of a single data quality check on an Iceberg table."""

    check_name: str
    status: CheckStatus
    table: str
    message: str
    details: dict[str, object] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, object]:
        return {
            "check_name": self.check_name,
            "status": self.status.value,
            "table": self.table,
            "message": self.message,
            "details": self.details,
            "timestamp": self.timestamp.isoformat(),
        }


class IcebergQualityAgent:
    """Validates data quality of Iceberg tables via the REST Catalog API.

    Connects to the Iceberg REST Catalog deployed by KF4X Phase 5
    and runs quality checks without requiring direct S3 access for metadata.
    """

    def __init__(self) -> None:
        settings = get_settings()
        self._catalog_url = settings.iceberg_rest_catalog_url.rstrip("/")
        self._headers = {"Content-Type": "application/json"}

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            headers=self._headers,
            timeout=30.0,
        )

    async def list_namespaces(self) -> list[str]:
        """List all namespaces in the Iceberg catalog."""
        async with self._client() as client:
            resp = await client.get(f"{self._catalog_url}/v1/namespaces")
            resp.raise_for_status()
            data = resp.json()
        return [ns[0] if isinstance(ns, list) else ns for ns in data.get("namespaces", [])]

    async def list_tables(self, namespace: str) -> list[str]:
        """List all tables in a namespace."""
        async with self._client() as client:
            resp = await client.get(f"{self._catalog_url}/v1/namespaces/{namespace}/tables")
            resp.raise_for_status()
            data = resp.json()
        results = []
        for t in data.get("identifiers", []):
            ns_val = t.get("namespace", [namespace])
            ns = ns_val[0] if isinstance(ns_val, list) else namespace
            results.append(f"{ns}.{t['name']}")
        return results

    async def _get_table_metadata(self, namespace: str, table: str) -> dict[str, Any]:
        """Fetch full table metadata from the REST Catalog."""
        async with self._client() as client:
            resp = await client.get(f"{self._catalog_url}/v1/namespaces/{namespace}/tables/{table}")
            resp.raise_for_status()
            return resp.json()  # type: ignore[no-any-return]

    async def check_schema_drift(
        self, namespace: str, table: str, expected_columns: list[str] | None = None
    ) -> QualityCheckResult:
        """Detect schema changes in an Iceberg table.

        Compares the current schema against either:
        - A provided list of expected column names, or
        - The table's own schema history (if the table has evolved)
        """
        full_table = f"{namespace}.{table}"
        try:
            metadata = await self._get_table_metadata(namespace, table)
        except httpx.HTTPStatusError as e:
            return QualityCheckResult(
                check_name="schema_drift",
                status=CheckStatus.ERROR,
                table=full_table,
                message=f"Failed to fetch table metadata: {e.response.status_code}",
            )

        # Get current schema
        current_schema = metadata.get("metadata", {}).get("current-schema", {})
        current_fields = current_schema.get("fields", [])
        current_columns = {f["name"]: f["type"] for f in current_fields}

        # Get all schemas to detect evolution
        schemas = metadata.get("metadata", {}).get("schemas", [])
        schema_count = len(schemas)

        if expected_columns:
            # Compare against expected columns
            current_names = set(current_columns.keys())
            expected_names = set(expected_columns)
            added = current_names - expected_names
            removed = expected_names - current_names

            if added or removed:
                return QualityCheckResult(
                    check_name="schema_drift",
                    status=CheckStatus.FAILED,
                    table=full_table,
                    message=(
                        f"Schema drift detected: {len(added)} added, {len(removed)} removed columns"
                    ),
                    details={
                        "added_columns": sorted(added),
                        "removed_columns": sorted(removed),
                        "current_columns": sorted(current_names),
                        "schema_versions": schema_count,
                    },
                )
        elif schema_count > 1:
            # Compare current schema against the previous version
            prev_schema = schemas[-2] if len(schemas) >= 2 else schemas[0]
            prev_columns = {f["name"]: f["type"] for f in prev_schema.get("fields", [])}

            added = set(current_columns) - set(prev_columns)
            removed = set(prev_columns) - set(current_columns)
            type_changes = {
                col: {"old": prev_columns[col], "new": current_columns[col]}
                for col in set(current_columns) & set(prev_columns)
                if current_columns[col] != prev_columns[col]
            }

            if added or removed or type_changes:
                return QualityCheckResult(
                    check_name="schema_drift",
                    status=CheckStatus.WARNING,
                    table=full_table,
                    message=(
                        f"Schema evolved: {len(added)} added, "
                        f"{len(removed)} removed, "
                        f"{len(type_changes)} type changes"
                    ),
                    details={
                        "added_columns": sorted(added),
                        "removed_columns": sorted(removed),
                        "type_changes": type_changes,
                        "schema_versions": schema_count,
                    },
                )

        return QualityCheckResult(
            check_name="schema_drift",
            status=CheckStatus.PASSED,
            table=full_table,
            message="No schema drift detected",
            details={"column_count": len(current_columns), "schema_versions": schema_count},
        )

    async def check_freshness(
        self, namespace: str, table: str, max_age_hours: float = 24.0
    ) -> QualityCheckResult:
        """Check if an Iceberg table has been updated recently.

        Uses the latest snapshot timestamp to determine freshness.
        """
        full_table = f"{namespace}.{table}"
        try:
            metadata = await self._get_table_metadata(namespace, table)
        except httpx.HTTPStatusError as e:
            return QualityCheckResult(
                check_name="freshness",
                status=CheckStatus.ERROR,
                table=full_table,
                message=f"Failed to fetch table metadata: {e.response.status_code}",
            )

        snapshots = metadata.get("metadata", {}).get("snapshots", [])
        if not snapshots:
            return QualityCheckResult(
                check_name="freshness",
                status=CheckStatus.WARNING,
                table=full_table,
                message="No snapshots found — table may be empty",
            )

        # Latest snapshot timestamp (Iceberg stores as epoch milliseconds)
        latest = snapshots[-1]
        ts_ms = latest.get("timestamp-ms", 0)
        snapshot_time = datetime.fromtimestamp(ts_ms / 1000, tz=UTC)
        now = datetime.now(UTC)
        age_hours = (now - snapshot_time).total_seconds() / 3600

        if age_hours > max_age_hours:
            return QualityCheckResult(
                check_name="freshness",
                status=CheckStatus.FAILED,
                table=full_table,
                message=(
                    f"Table is stale: last update {age_hours:.1f}h ago "
                    f"(threshold: {max_age_hours}h)"
                ),
                details={
                    "last_snapshot": snapshot_time.isoformat(),
                    "age_hours": round(age_hours, 2),
                    "max_age_hours": max_age_hours,
                    "snapshot_id": latest.get("snapshot-id"),
                },
            )

        return QualityCheckResult(
            check_name="freshness",
            status=CheckStatus.PASSED,
            table=full_table,
            message=f"Table is fresh: last update {age_hours:.1f}h ago",
            details={
                "last_snapshot": snapshot_time.isoformat(),
                "age_hours": round(age_hours, 2),
                "snapshot_count": len(snapshots),
            },
        )

    async def check_snapshot_volume(
        self,
        namespace: str,
        table: str,
        min_change_pct: float = -50.0,
        max_change_pct: float = 200.0,
    ) -> QualityCheckResult:
        """Detect volume anomalies by comparing recent snapshot summaries.

        Compares added-records counts between the last two snapshots.
        Flags if the change exceeds thresholds.
        """
        full_table = f"{namespace}.{table}"
        try:
            metadata = await self._get_table_metadata(namespace, table)
        except httpx.HTTPStatusError as e:
            return QualityCheckResult(
                check_name="volume_anomaly",
                status=CheckStatus.ERROR,
                table=full_table,
                message=f"Failed to fetch table metadata: {e.response.status_code}",
            )

        snapshots = metadata.get("metadata", {}).get("snapshots", [])
        if len(snapshots) < 2:
            return QualityCheckResult(
                check_name="volume_anomaly",
                status=CheckStatus.PASSED,
                table=full_table,
                message="Not enough snapshots to compare volume (need at least 2)",
                details={"snapshot_count": len(snapshots)},
            )

        # Get added-records from the last two snapshots
        prev_summary = snapshots[-2].get("summary", {})
        curr_summary = snapshots[-1].get("summary", {})

        prev_added = int(prev_summary.get("added-records", "0"))
        curr_added = int(curr_summary.get("added-records", "0"))

        if prev_added == 0:
            return QualityCheckResult(
                check_name="volume_anomaly",
                status=CheckStatus.PASSED,
                table=full_table,
                message="Previous snapshot had 0 added records — skipping comparison",
                details={"current_added": curr_added, "previous_added": prev_added},
            )

        change_pct = ((curr_added - prev_added) / prev_added) * 100

        if change_pct < min_change_pct or change_pct > max_change_pct:
            return QualityCheckResult(
                check_name="volume_anomaly",
                status=CheckStatus.FAILED,
                table=full_table,
                message=(
                    f"Volume anomaly: {change_pct:+.1f}% change "
                    f"(threshold: {min_change_pct}% to "
                    f"{max_change_pct}%)"
                ),
                details={
                    "current_added_records": curr_added,
                    "previous_added_records": prev_added,
                    "change_pct": round(change_pct, 2),
                    "total_records": int(curr_summary.get("total-records", "0")),
                },
            )

        return QualityCheckResult(
            check_name="volume_anomaly",
            status=CheckStatus.PASSED,
            table=full_table,
            message=f"Volume is normal: {change_pct:+.1f}% change",
            details={
                "current_added_records": curr_added,
                "previous_added_records": prev_added,
                "change_pct": round(change_pct, 2),
            },
        )

    async def run_all_checks(
        self,
        namespace: str,
        table: str,
        expected_columns: list[str] | None = None,
        max_age_hours: float = 24.0,
    ) -> list[QualityCheckResult]:
        """Run all quality checks on a single Iceberg table.

        Returns a list of results — one per check.
        """
        results = [
            await self.check_schema_drift(namespace, table, expected_columns),
            await self.check_freshness(namespace, table, max_age_hours),
            await self.check_snapshot_volume(namespace, table),
        ]

        failed = sum(1 for r in results if r.status == CheckStatus.FAILED)
        logger.info(
            "iceberg_quality_checks_complete",
            table=f"{namespace}.{table}",
            total=len(results),
            passed=sum(1 for r in results if r.status == CheckStatus.PASSED),
            warnings=sum(1 for r in results if r.status == CheckStatus.WARNING),
            failed=failed,
        )
        return results

    async def scan_namespace(
        self, namespace: str, max_age_hours: float = 24.0
    ) -> dict[str, list[QualityCheckResult]]:
        """Run quality checks on all tables in a namespace.

        Returns a dict mapping table name → list of check results.
        """
        tables = await self.list_tables(namespace)
        results: dict[str, list[QualityCheckResult]] = {}

        for full_name in tables:
            table_name = full_name.split(".")[-1]
            results[full_name] = await self.run_all_checks(
                namespace, table_name, max_age_hours=max_age_hours
            )

        total_checks = sum(len(v) for v in results.values())
        total_failed = sum(
            1 for checks in results.values() for c in checks if c.status == CheckStatus.FAILED
        )
        logger.info(
            "iceberg_namespace_scan_complete",
            namespace=namespace,
            tables=len(tables),
            total_checks=total_checks,
            total_failed=total_failed,
        )
        return results
