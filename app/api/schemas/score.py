from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class DistributionSpec(BaseModel):
    type: Literal["normal", "lognormal", "triangular", "point"] = "point"
    mean: float | None = None
    sd: float | None = Field(default=None, ge=0)
    low: float | None = None
    mode: float | None = None
    high: float | None = None
    value: float | None = None

    @model_validator(mode="after")
    def validate_shape(self) -> "DistributionSpec":
        if self.type == "point":
            if self.value is None and self.mean is None:
                raise ValueError("point distribution requires `value` or `mean`")
            return self
        if self.type == "normal" and (self.mean is None or self.sd is None):
            raise ValueError("normal distribution requires `mean` and `sd`")
        if self.type == "lognormal" and (self.mean is None or self.sd is None):
            raise ValueError("lognormal distribution requires `mean` and `sd`")
        if self.type == "triangular" and (self.low is None or self.mode is None or self.high is None):
            raise ValueError("triangular distribution requires `low`, `mode`, and `high`")
        return self


UpliftSpec = float | DistributionSpec
RunPurpose = Literal["baseline", "refresh", "scenario_test", "review", "approval", "what_if"]
RunStatus = Literal["success", "failed"]


class SegmentInput(BaseModel):
    id: str = Field(min_length=1)
    penetration: float = Field(ge=0.0, le=1.0)
    screen_penetration: dict[str, float] | None = None
    uplifts: dict[str, UpliftSpec] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_screen_penetration(self) -> "SegmentInput":
        if self.screen_penetration:
            for screen, value in self.screen_penetration.items():
                if value < 0.0 or value > 1.0:
                    raise ValueError(f"screen_penetration `{screen}` must be in [0,1]")
        return self


class MetricTargetInput(BaseModel):
    node: str = Field(min_length=1)
    uplift_dist: UpliftSpec
    metric_key: str | None = None
    node_type: Literal["metric", "funnel_step"] = "metric"
    target_id: str | None = None

    @model_validator(mode="after")
    def validate_target(self) -> "MetricTargetInput":
        if self.node_type == "funnel_step" and not self.target_id:
            raise ValueError("funnel_step target requires `target_id`")
        return self


class CannibalizationInput(BaseModel):
    mode: Literal["off", "matrix"] = "off"
    matrix_id: str | None = None
    conservative_shrink: float = Field(default=0.0, ge=0.0, le=1.0)


class InteractionInput(BaseModel):
    with_initiative_id: str
    composition: Literal["multiplicative"] = "multiplicative"


class MonteCarloInput(BaseModel):
    n: int = Field(default=10_000, ge=100, le=50_000)
    seed: int = Field(default=123, ge=0)
    enabled: bool = True


class InputVersions(BaseModel):
    baseline_metrics: str | None = None
    baseline_funnel_steps: str | None = None
    cannibalization_matrix: str | None = None
    scoring_policy: str | None = None


class MetricTreeSelector(BaseModel):
    template_name: str
    version: str | None = None


class ScoringPolicySelector(BaseModel):
    name: str
    version: str | None = None


class DecayConfig(BaseModel):
    type: Literal["no_decay", "exponential", "linear"] = "no_decay"
    half_life_weeks: float | None = Field(default=None, gt=0)
    linear_floor: float = Field(default=0.0, ge=0.0, le=1.0)

    @model_validator(mode="after")
    def validate_decay(self) -> "DecayConfig":
        if self.type == "exponential" and self.half_life_weeks is None:
            raise ValueError("exponential decay requires `half_life_weeks`")
        return self


class SensitivityConfig(BaseModel):
    enabled: bool = False
    epsilon: float = Field(default=0.1, gt=0, le=1.0)
    top_n: int = Field(default=10, ge=1, le=50)
    target_metric: Literal["net_margin", "priority_score"] = "net_margin"


class LearningConfig(BaseModel):
    mode: Literal["off", "advisory", "bayesian"] = "bayesian"
    lookback_days: int = Field(default=730, ge=1, le=3650)
    half_life_days: int = Field(default=180, ge=1, le=3650)
    min_quality: float = Field(default=0.6, ge=0.0, le=1.0)
    min_sample_size: int = Field(default=500, ge=1, le=10_000_000)


class ScenarioOverride(BaseModel):
    segments: list[SegmentInput] | None = None
    metric_targets: list[MetricTargetInput] | None = None
    p_success: float | None = Field(default=None, ge=0.0, le=1.0)
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    evidence_type: str | None = None
    cannibalization: CannibalizationInput | None = None
    decay: DecayConfig | None = None


class ActorOverride(BaseModel):
    user_id: str | None = None
    email: str | None = None
    role: str | None = None


class ScoreRunRequest(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "initiative_name": "Improve checkout UX",
                "segments": [
                    {"id": "new_users", "penetration": 0.3, "screen_penetration": {"home": 0.8}, "uplifts": {"conversion": {"type": "normal", "mean": 0.05, "sd": 0.02}}},
                    {"id": "returning_users", "penetration": 0.2, "uplifts": {"frequency_monthly": 0.03}},
                ],
                "screens": ["home", "catalog", "search"],
                "metric_targets": [{"node": "conversion", "node_type": "metric", "uplift_dist": {"type": "point", "value": 0.04}}],
                "p_success": 0.7,
                "evidence_type": "ab_test",
                "effort_cost": 25000,
                "strategic_weight": 1.2,
                "learning_value": 1.1,
                "baseline_window": "quarter",
                "horizon_weeks": 26,
                "horizons_weeks": [4, 13, 26, 52],
                "decay": {"type": "exponential", "half_life_weeks": 13},
                "discount_rate_annual": 0.1,
                "cannibalization": {"mode": "off"},
                "monte_carlo": {"n": 10000, "seed": 123, "enabled": True},
            }
        }
    )

    initiative_id: str | None = None
    initiative_name: str = Field(min_length=1)
    data_scope: str = Field(default="prod", min_length=1, max_length=64)
    segments: list[SegmentInput] = Field(min_length=1)
    screens: list[str] = Field(min_length=1)
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
    horizon_weeks: int = Field(default=26, ge=1, le=520)
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

    @model_validator(mode="after")
    def validate_horizons(self) -> "ScoreRunRequest":
        if self.horizons_weeks:
            for horizon in self.horizons_weeks:
                if horizon < 1 or horizon > 520:
                    raise ValueError("horizons_weeks values must be in [1,520]")
        if (self.baseline_date_start is None) != (self.baseline_date_end is None):
            raise ValueError("baseline_date_start and baseline_date_end must be provided together")
        if self.baseline_date_start and self.baseline_date_end and self.baseline_date_start > self.baseline_date_end:
            raise ValueError("baseline_date_start must be <= baseline_date_end")
        return self


class ScoreRunCreateV11(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "initiative_id": "f927f680-8f17-4e58-8e10-153cc40f0ea1",
                    "run_label": "Q2 refresh",
                    "run_purpose": "refresh",
                },
                {
                    "initiative_version_id": "25ad26ed-e4cb-4020-9def-e1796eb4f73f",
                    "run_label": "approval packet",
                    "run_purpose": "approval",
                },
            ]
        }
    )

    initiative_id: str | None = None
    initiative_version_id: str | None = None
    data_scope: str | None = Field(default=None, min_length=1, max_length=64)
    save_as_new_version: bool = False
    version_change_comment: str | None = None
    run_label: str | None = None
    run_purpose: RunPurpose | None = None
    actor_override: ActorOverride | None = None

    initiative_name: str | None = None
    segments: list[SegmentInput] | None = None
    screens: list[str] | None = None
    metric_targets: list[MetricTargetInput] | None = None
    p_success: float | None = Field(default=None, ge=0.0, le=1.0)
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    evidence_type: str | None = None
    effort_cost: float | None = Field(default=None, gt=0)
    strategic_weight: float | None = Field(default=None, ge=0)
    learning_value: float | None = Field(default=None, ge=0)
    baseline_window: Literal["month", "quarter", "half_year", "year"] | None = None
    baseline_date_start: date | None = None
    baseline_date_end: date | None = None
    horizon_weeks: int | None = Field(default=None, ge=1, le=520)
    horizons_weeks: list[int] | None = None
    decay: DecayConfig | None = None
    discount_rate_annual: float | None = Field(default=None, ge=0)
    cannibalization: CannibalizationInput | None = None
    interactions: list[InteractionInput] | None = None
    monte_carlo: MonteCarloInput | None = None
    scenarios: dict[str, ScenarioOverride] | None = None
    sensitivity: SensitivityConfig | None = None
    learning: LearningConfig | None = None
    input_versions: InputVersions | None = None
    metric_tree: MetricTreeSelector | None = None
    scoring_policy: ScoringPolicySelector | None = None

    @property
    def has_ad_hoc_payload(self) -> bool:
        required = [
            self.initiative_name,
            self.segments,
            self.screens,
            self.p_success,
            self.effort_cost,
            self.horizon_weeks,
        ]
        return all(value is not None for value in required)

    @model_validator(mode="after")
    def validate_mode(self) -> "ScoreRunCreateV11":
        if self.initiative_version_id:
            return self
        if self.has_ad_hoc_payload:
            if self.save_as_new_version and not self.initiative_id:
                raise ValueError("save_as_new_version=true requires initiative_id")
            return self
        if self.initiative_id:
            return self
        raise ValueError(
            "Provide ad hoc scoring payload, or initiative_id, or initiative_version_id"
        )

    def to_score_run_request(self) -> ScoreRunRequest:
        if not self.has_ad_hoc_payload:
            raise ValueError("Ad hoc scoring payload is not complete")
        return ScoreRunRequest(
            initiative_id=self.initiative_id,
            initiative_name=self.initiative_name or "",
            data_scope=self.data_scope or "prod",
            segments=self.segments or [],
            screens=self.screens or [],
            metric_targets=self.metric_targets or [],
            p_success=self.p_success if self.p_success is not None else 0.0,
            confidence=self.confidence,
            evidence_type=self.evidence_type,
            effort_cost=self.effort_cost if self.effort_cost is not None else 1.0,
            strategic_weight=self.strategic_weight if self.strategic_weight is not None else 1.0,
            learning_value=self.learning_value if self.learning_value is not None else 1.0,
            baseline_window=self.baseline_window or "quarter",
            baseline_date_start=self.baseline_date_start,
            baseline_date_end=self.baseline_date_end,
            horizon_weeks=self.horizon_weeks if self.horizon_weeks is not None else 26,
            horizons_weeks=self.horizons_weeks,
            decay=self.decay,
            discount_rate_annual=self.discount_rate_annual,
            cannibalization=self.cannibalization or CannibalizationInput(),
            interactions=self.interactions or [],
            monte_carlo=self.monte_carlo or MonteCarloInput(),
            scenarios=self.scenarios,
            sensitivity=self.sensitivity or SensitivityConfig(),
            learning=self.learning,
            input_versions=self.input_versions,
            metric_tree=self.metric_tree,
            scoring_policy=self.scoring_policy,
        )


class DeterministicImpact(BaseModel):
    incremental_rto: float
    incremental_fm: float
    incremental_gmv: float
    incremental_margin: float
    incremental_orders: float
    incremental_items: float
    incremental_aoq: float
    incremental_aov: float
    expected_value: float
    expected_rto: float | None = None
    expected_fm: float | None = None
    expected_gmv: float | None = None
    expected_margin: float
    roi: float
    priority_score: float
    bet_size: Literal["small", "medium", "large"]
    uncertainty_tag: Literal["low", "medium", "high"]


class HistogramBin(BaseModel):
    lower: float
    upper: float
    count: int


class ProbabilisticSummary(BaseModel):
    mean: float
    median: float
    p5: float
    p95: float
    prob_negative: float
    stddev: float
    cv: float
    histogram: list[HistogramBin]


class ImpactBreakdown(BaseModel):
    orders: float
    items: float
    gmv: float
    margin: float
    rto: float | None = None
    fm: float | None = None
    reallocated_orders: float = 0.0
    reallocated_items: float = 0.0
    reallocated_gmv: float = 0.0
    reallocated_margin: float = 0.0
    reallocated_rto: float | None = None
    reallocated_fm: float | None = None


class HorizonResult(BaseModel):
    deterministic: dict[str, Any]
    probabilistic: dict[str, Any]
    gross_impact: ImpactBreakdown
    net_incremental_impact: ImpactBreakdown
    discounted_summary: dict[str, Any] | None = None


class ScenarioResult(BaseModel):
    deterministic: dict[str, Any]
    probabilistic: dict[str, Any]
    gross_impact: ImpactBreakdown
    net_incremental_impact: ImpactBreakdown
    horizon_results: dict[str, HorizonResult]


class SensitivityEntry(BaseModel):
    input: str
    elasticity: float
    delta_value: float


class SensitivityOutput(BaseModel):
    top_sensitive_inputs: list[SensitivityEntry]
    elasticity_summary: dict[str, float]
    tornado: list[SensitivityEntry]


class ExplainabilityOutput(BaseModel):
    top_segments: list[dict[str, Any]]
    top_screens: list[dict[str, Any]]
    top_nodes: list[dict[str, Any]]
    primary_driver: str
    largest_risk_driver: str | None
    cannibalization_summary: str
    historical_evidence_summary: str | None = None
    summary_text: str


class LearningSummary(BaseModel):
    prior_mean: float
    prior_std: float
    posterior_mean: float
    posterior_std: float
    evidence_count: int
    evidence_ids: list[str]


class ScoreRunResponse(BaseModel):
    run_id: str
    assumptions_snapshot_hash: str
    resolved_versions: dict[str, str]
    code_version: str
    seed: int
    deterministic: DeterministicImpact
    probabilistic: ProbabilisticSummary
    per_segment: dict[str, dict[str, float]]
    per_metric_node: dict[str, float]

    gross_impact: ImpactBreakdown | None = None
    net_incremental_impact: ImpactBreakdown | None = None
    horizon_results: dict[str, HorizonResult] | None = None
    scenarios: dict[str, ScenarioResult] | None = None
    scenario_comparison: dict[str, dict[str, float]] | None = None
    sensitivity: SensitivityOutput | None = None
    explainability: ExplainabilityOutput | None = None
    effective_input_metrics: list[str] | None = None
    derived_output_metrics: list[str] | None = None
    validation_warnings: list[str] | None = None
    learning_applied: bool | None = None
    learning_summary: LearningSummary | None = None
    learning_warnings: list[str] | None = None
    scoring_policy_version: str | None = None
    scoring_policy_source: str | None = None
    per_screen_breakdown: dict[str, dict[str, float]] | None = None


class ScoringRunRecord(BaseModel):
    id: str
    initiative_id: str | None
    initiative_version_id: str | None
    initiative_name: str
    assumptions_snapshot_hash: str
    rng_seed: int
    monte_carlo_n: int
    code_version: str
    created_by: str
    triggered_by_user_id: str | None
    triggered_by_email: str | None
    triggered_by_role: str | None
    run_label: str | None
    run_purpose: RunPurpose | None
    run_status: RunStatus
    error_message: str | None
    scenario_names: list[str]
    created_at: datetime
    recompute_of_run_id: str | None
    deterministic_output: dict[str, Any]
    probabilistic_output: dict[str, Any]


class ScoringRunListResponse(BaseModel):
    items: list[ScoringRunRecord]


class ScoringRunDetailResponse(BaseModel):
    id: str
    initiative_id: str | None
    initiative_version_id: str | None
    initiative_name: str
    request_payload: dict[str, Any]
    resolved_inputs: dict[str, Any]
    assumptions_snapshot_hash: str
    rng_seed: int
    monte_carlo_n: int
    code_version: str
    deterministic_output: dict[str, Any]
    probabilistic_output: dict[str, Any]
    segment_breakdown: dict[str, Any]
    node_contributions: dict[str, Any]
    created_by: str
    triggered_by_user_id: str | None
    triggered_by_email: str | None
    triggered_by_role: str | None
    run_label: str | None
    run_purpose: RunPurpose | None
    run_status: RunStatus
    error_message: str | None
    scenario_names: list[str]
    per_screen_breakdown: dict[str, dict[str, float]] | None = None
    created_at: datetime
    recompute_of_run_id: str | None
