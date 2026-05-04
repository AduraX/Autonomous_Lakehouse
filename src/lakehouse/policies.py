"""Static policy classification for remediation actions.

Classifies action types into tiers that determine whether they can be
auto-approved, require human approval, or are forbidden entirely.
Ported from agentic-orch's action engine design.
"""

from enum import StrEnum

from lakehouse.logging import get_logger
from lakehouse.models.actions import ActionStatus, ActionType

logger = get_logger(__name__)


class PolicyClassification(StrEnum):
    AUTO_APPROVED = "auto_approved"
    REQUIRES_APPROVAL = "requires_approval"
    FORBIDDEN = "forbidden"


class PolicyViolationError(ValueError):
    """Raised when a forbidden action is requested."""

    def __init__(self, action_type: ActionType) -> None:
        self.action_type = action_type
        super().__init__(f"Action type '{action_type.value}' is forbidden by policy")


# --- Action tier definitions ---

AUTO_APPROVED_ACTIONS: frozenset[ActionType] = frozenset(
    {
        ActionType.NOTIFY_OWNER,
    }
)

APPROVAL_REQUIRED_ACTIONS: frozenset[ActionType] = frozenset(
    {
        ActionType.RETRY_PIPELINE,
        ActionType.BLOCK_DOWNSTREAM,
        ActionType.SCALE_COMPUTE,
        ActionType.REFRESH_CREDENTIALS,
        ActionType.ADJUST_SCHEDULE,
        ActionType.REQUEST_APPROVAL,
        ActionType.CUSTOM,
    }
)

FORBIDDEN_ACTIONS: frozenset[ActionType] = frozenset()


def classify_action(action_type: ActionType) -> PolicyClassification:
    """Classify an action type into a policy tier."""
    if action_type in FORBIDDEN_ACTIONS:
        return PolicyClassification.FORBIDDEN
    if action_type in AUTO_APPROVED_ACTIONS:
        return PolicyClassification.AUTO_APPROVED
    if action_type in APPROVAL_REQUIRED_ACTIONS:
        return PolicyClassification.REQUIRES_APPROVAL
    # Unknown action types default to requiring approval
    return PolicyClassification.REQUIRES_APPROVAL


def apply_policy(action_type: ActionType) -> ActionStatus:
    """Return the initial ActionStatus based on policy classification.

    Raises PolicyViolationError for forbidden actions.
    """
    classification = classify_action(action_type)

    logger.info(
        "policy_evaluated",
        action_type=action_type.value,
        classification=classification.value,
    )

    if classification == PolicyClassification.FORBIDDEN:
        raise PolicyViolationError(action_type)
    if classification == PolicyClassification.AUTO_APPROVED:
        return ActionStatus.APPROVED
    return ActionStatus.PROPOSED
