"""Action API routes — propose, approve, execute remediation actions."""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from lakehouse.adapters import get_adapter
from lakehouse.agents.action_executor import ActionExecutor
from lakehouse.api.dependencies import get_db
from lakehouse.logging import get_logger
from lakehouse.models.actions import Action
from lakehouse.policies import PolicyViolationError, apply_policy
from lakehouse.schemas.actions import ActionCreate, ActionResponse, ActionUpdateStatus

router = APIRouter()
logger = get_logger(__name__)


@router.post("/actions", response_model=ActionResponse, status_code=201)
async def propose_action(
    payload: ActionCreate,
    session: AsyncSession = Depends(get_db),
) -> Action:
    """Propose a new remediation action for a diagnosis."""
    try:
        initial_status = apply_policy(payload.action_type)
    except PolicyViolationError as err:
        raise HTTPException(status_code=403, detail=str(err)) from err

    action = Action(**payload.model_dump(), status=initial_status)
    session.add(action)
    await session.flush()
    await session.refresh(action)

    logger.info(
        "action_proposed",
        action_id=action.id,
        diagnosis_id=action.diagnosis_id,
        action_type=action.action_type,
    )
    return action


@router.get("/actions/{action_id}", response_model=ActionResponse)
async def get_action(
    action_id: int,
    session: AsyncSession = Depends(get_db),
) -> Action:
    """Retrieve an action by ID."""
    action = await session.get(Action, action_id)
    if not action:
        raise HTTPException(status_code=404, detail="Action not found")
    return action


@router.patch("/actions/{action_id}", response_model=ActionResponse)
async def update_action_status(
    action_id: int,
    payload: ActionUpdateStatus,
    session: AsyncSession = Depends(get_db),
) -> Action:
    """Update an action's status (approve, reject, mark as executed, etc.)."""
    action = await session.get(Action, action_id)
    if not action:
        raise HTTPException(status_code=404, detail="Action not found")

    for field, value in payload.model_dump(exclude_unset=True).items():
        setattr(action, field, value)

    await session.flush()
    await session.refresh(action)

    logger.info(
        "action_status_updated",
        action_id=action.id,
        new_status=action.status,
    )
    return action


@router.post("/actions/{action_id}/execute", response_model=ActionResponse)
async def execute_action(
    action_id: int,
    session: AsyncSession = Depends(get_db),
) -> Action:
    """Execute an approved action synchronously.

    Runs the action via the platform adapter with exponential backoff retry.
    The action must be in APPROVED status.
    """
    adapter = get_adapter()
    executor = ActionExecutor(
        adapter=adapter,
        session=session,
        backoff_base=0.1,  # fast retries for API calls
    )

    try:
        result = await executor.execute(action_id)
    except ValueError as err:
        raise HTTPException(status_code=400, detail=str(err)) from err

    action = await session.get(Action, action_id)
    if not action:
        raise HTTPException(status_code=404, detail="Action not found")

    logger.info(
        "action_execution_requested",
        action_id=action_id,
        status=result.status.value,
        attempts=result.attempts,
    )
    return action


@router.get("/diagnoses/{diagnosis_id}/actions", response_model=list[ActionResponse])
async def list_diagnosis_actions(
    diagnosis_id: int,
    session: AsyncSession = Depends(get_db),
) -> list[Action]:
    """List all actions for a specific diagnosis."""
    result = await session.execute(
        select(Action).where(Action.diagnosis_id == diagnosis_id).order_by(Action.created_at.desc())
    )
    return list(result.scalars().all())
