from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from app.api.schemas.score import (
    CannibalizationInput,
    DecayConfig,
    InputVersions,
    InteractionInput,
    MetricTargetInput,
    MetricTreeSelector,
    MonteCarloInput,
    ScenarioOverride,
    ScoringPolicySelector,
    SegmentInput,
    SensitivityConfig,
    LearningConfig,
)
from app.api.schemas.team import TeamRead

InitiativeStatus = Literal["draft", "active", "archived"]


class InitiativeVersionPayload(BaseModel):
    title_override: str | None = Field(default=None, max_length=255)
    description_override: str | None = Field(default=None, max_length=4096)
    data_scope: str = Field(default="prod", min_length=1, max_length=64)
    screens: list[str] = Field(min_length=1)
    segments: list[SegmentInput] = Field(min_length=1)
    metric_targets: list[MetricTargetInput] = Field(default_factory=list)

    p_success: float = Field(ge=0.0, le=1.0)
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    evidence_type: str | None = None
    effort_cost: float = Field(gt=0)
    strategic_weight: float = Field(default=1.0, ge=0)
    learning_value: float = Field(default=1.0, ge=0)
    baseline_window: Literal["month", "quarter", "half_year", "year"] = "quarter"
    baseline_date_start: date | None = None
    baseline_date_end: date | None = None
    horizon_weeks: int = Field(ge=1, le=520)
    horizons_weeks: list[int] | None = None
    decay: DecayConfig | None = None
    discount_rate_annual: float | None = Field(default=None, ge=0)
    cannibalization: CannibalizationInput = Field(default_factory=CannibalizationInput)
    interactions: list[InteractionInput] = Field(default_factory=list)
    monte_carlo: MonteCarloInput = Field(default_factory=MonteCarloInput)
    scenarios: dict[str, ScenarioOverride] | None = None
    sensitivity: SensitivityConfig = Field(default_factory=SensitivityConfig)
    learning: LearningConfig | None = None
    input_versions: InputVersions | None = None
    metric_tree: MetricTreeSelector | None = None
    scoring_policy: ScoringPolicySelector | None = None


class InitiativeVersionCreate(InitiativeVersionPayload):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "change_comment": "Refined search funnel assumptions",
                "title_override": "Checkout v2",
                "description_override": "Adjusted post-discovery estimates",
                "screens": ["home", "catalog", "search"],
                "segments": [{"id": "new_users", "penetration": 0.35, "uplifts": {"conversion": 0.06}}],
                "metric_targets": [],
                "p_success": 0.7,
                "confidence": 0.75,
                "effort_cost": 30000,
                "strategic_weight": 1.2,
                "learning_value": 1.1,
                "horizon_weeks": 26,
            }
        }
    )
    change_comment: str | None = Field(default=None, max_length=4096)


class InitiativeVersionRead(InitiativeVersionPayload):
    id: str
    initiative_id: str
    version_number: int
    assumptions_json: dict[str, Any]
    created_by_user_id: str | None
    created_by_email: str | None
    change_comment: str | None
    created_at: datetime


class InitiativeVersionSummary(BaseModel):
    id: str
    version_number: int
    title_override: str | None
    change_comment: str | None
    created_at: datetime


class InitiativeCreate(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        json_schema_extra={
            "example": {
                "external_key": "checkout-redesign",
                "name": "Checkout Redesign",
                "description": "Reduce drop-off in payment step",
                "status": "active",
                "owner_team_id": "c5f5d848-5c7e-4f0b-a8a8-9cf31374668d",
                "tags": {"portfolio": "growth"},
                "initial_version": {
                    "change_comment": "Initial estimate",
                    "screens": ["home", "catalog", "search"],
                    "segments": [{"id": "new_users", "penetration": 0.3, "uplifts": {"conversion": 0.05}}],
                    "metric_targets": [],
                    "p_success": 0.7,
                    "confidence": 0.75,
                    "effort_cost": 30000,
                    "strategic_weight": 1.2,
                    "learning_value": 1.1,
                    "horizon_weeks": 26,
                },
            }
        },
    )

    external_key: str | None = Field(default=None, alias="external_id")
    name: str = Field(min_length=1, max_length=255)
    description: str | None = Field(default=None, max_length=4096)
    status: InitiativeStatus = "draft"
    owner_team_id: str = Field(min_length=1)
    tags: dict[str, Any] = Field(default_factory=dict)
    initial_version: InitiativeVersionCreate | None = None


class InitiativeUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=255)
    description: str | None = Field(default=None, max_length=4096)
    status: InitiativeStatus | None = None
    owner_team_id: str | None = Field(default=None, min_length=1)


class InitiativeCreator(BaseModel):
    user_id: str | None
    email: str | None


class InitiativeRunSummary(BaseModel):
    run_id: str
    created_at: datetime
    triggered_by_user_id: str | None
    triggered_by_email: str | None
    run_purpose: str | None


class InitiativeLatestRunMetrics(BaseModel):
    expected_gmv: float | None = None
    expected_margin: float | None = None
    expected_rto: float | None = None
    expected_fm: float | None = None
    roi: float | None = None
    priority_score: float | None = None
    prob_negative: float | None = None
    uncertainty_tag: str | None = None
    run_id: str | None = None
    run_created_at: datetime | None = None


class InitiativeRead(BaseModel):
    id: str
    external_key: str | None
    name: str
    description: str | None
    status: InitiativeStatus
    owner_team_id: str
    owner_team: TeamRead | None = None
    created_by: InitiativeCreator
    tags: dict[str, Any]
    created_at: datetime
    updated_at: datetime
    archived_at: datetime | None
    latest_version_number: int | None
    versions_count: int
    last_scored_at: datetime | None
    last_scored_by: str | None
    runs_count: int
    latest_version_summary: InitiativeVersionSummary | None = None
    last_run_summary: InitiativeRunSummary | None = None
    latest_run_metrics: InitiativeLatestRunMetrics | None = None


class InitiativeListResponse(BaseModel):
    items: list[InitiativeRead]


class InitiativeVersionListResponse(BaseModel):
    items: list[InitiativeVersionRead]


class InitiativeCompareResponse(BaseModel):
    initiative_id: str
    version_a: str
    version_b: str
    assumptions_diff: dict[str, dict[str, Any]]
    outputs_available: bool
    outputs_a: dict[str, Any] | None
    outputs_b: dict[str, Any] | None
    outputs_delta: dict[str, float] | None
