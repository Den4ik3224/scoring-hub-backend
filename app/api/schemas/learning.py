from datetime import datetime

from pydantic import BaseModel, Field, model_validator


class ABResultCreate(BaseModel):
    experiment_id: str = Field(min_length=1, max_length=255)
    initiative_id: str | None = Field(default=None, min_length=1, max_length=36)
    scope: str = Field(default="prod", min_length=1, max_length=64)
    screen: str = Field(min_length=1, max_length=128)
    segment_id: str | None = Field(default=None, min_length=1, max_length=128)
    metric_driver: str = Field(min_length=1, max_length=128)
    observed_uplift: float
    ci_low: float | None = None
    ci_high: float | None = None
    sample_size: int = Field(ge=1)
    significance_flag: bool = False
    quality_score: float = Field(ge=0.0, le=1.0)
    source: str = Field(min_length=1, max_length=255)
    start_at: datetime
    end_at: datetime

    @model_validator(mode="after")
    def validate_ci(self) -> "ABResultCreate":
        if self.ci_low is not None and self.ci_high is not None and self.ci_low > self.ci_high:
            raise ValueError("ci_low must be <= ci_high")
        if self.start_at > self.end_at:
            raise ValueError("start_at must be <= end_at")
        return self


class ABResultRead(ABResultCreate):
    id: str
    created_by: str
    created_at: datetime


class ABResultListResponse(BaseModel):
    items: list[ABResultRead]
