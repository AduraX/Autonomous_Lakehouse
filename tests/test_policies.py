"""Tests for the static policy classification layer."""

import pytest

from lakehouse.models.actions import ActionStatus, ActionType
from lakehouse.policies import (
    PolicyClassification,
    PolicyViolationError,
    apply_policy,
    classify_action,
)


def test_notify_owner_is_auto_approved() -> None:
    assert classify_action(ActionType.NOTIFY_OWNER) == PolicyClassification.AUTO_APPROVED


@pytest.mark.parametrize(
    "action_type",
    [
        ActionType.RETRY_PIPELINE,
        ActionType.BLOCK_DOWNSTREAM,
        ActionType.SCALE_COMPUTE,
        ActionType.REFRESH_CREDENTIALS,
        ActionType.ADJUST_SCHEDULE,
        ActionType.REQUEST_APPROVAL,
        ActionType.CUSTOM,
    ],
)
def test_approval_required_actions(action_type: ActionType) -> None:
    assert classify_action(action_type) == PolicyClassification.REQUIRES_APPROVAL


def test_apply_policy_auto_approved_returns_approved() -> None:
    assert apply_policy(ActionType.NOTIFY_OWNER) == ActionStatus.APPROVED


def test_apply_policy_requires_approval_returns_proposed() -> None:
    assert apply_policy(ActionType.RETRY_PIPELINE) == ActionStatus.PROPOSED


def test_policy_violation_error_message() -> None:
    err = PolicyViolationError(ActionType.RETRY_PIPELINE)
    assert "retry_pipeline" in str(err)
    assert err.action_type == ActionType.RETRY_PIPELINE


def test_all_action_types_classifiable() -> None:
    """Every ActionType should return a valid classification without error."""
    for at in ActionType:
        result = classify_action(at)
        assert isinstance(result, PolicyClassification)


def test_apply_policy_forbidden_raises() -> None:
    """Forbidden actions should raise PolicyViolationError."""
    from lakehouse import policies

    original = policies.FORBIDDEN_ACTIONS
    policies.FORBIDDEN_ACTIONS = frozenset({ActionType.CUSTOM})
    try:
        with pytest.raises(PolicyViolationError):
            apply_policy(ActionType.CUSTOM)
    finally:
        policies.FORBIDDEN_ACTIONS = original
