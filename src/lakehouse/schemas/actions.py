"""Pydantic schemas for Actions."""

from datetime import datetime

from pydantic import BaseModel, Field

from lakehouse.models.actions import ActionStatus, ActionType


class ActionCreate(BaseModel):
    """Schema for proposing a new action."""

    diagnosis_id: int
    action_type: ActionType
    description: str = Field(..., examples=["Retry pipeline with updated schema mapping"])
    parameters_json: str | None = None
    executed_by: str = Field(default="system", max_length=100)


class ActionUpdateStatus(BaseModel):
    """Schema for updating an action's status (approve/reject/execute)."""

    status: ActionStatus
    approved_by: str | None = None
    result_json: str | None = None
    error_message: str | None = None


class ActionResponse(BaseModel):
    """Schema returned when reading an action."""

    model_config = {"from_attributes": True}

    id: int
    diagnosis_id: int
    action_type: ActionType
    description: str
    parameters_json: str | None
    status: ActionStatus
    result_json: str | None
    error_message: str | None
    approved_by: str | None
    executed_by: str
    created_at: datetime
    updated_at: datetime
