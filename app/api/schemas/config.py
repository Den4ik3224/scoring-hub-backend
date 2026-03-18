from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from app.api.schemas.score import (
    CannibalizationInput,
    DecayConfig,
    InputVersions,
    InteractionInput,
    LearningConfig,
    MetricTargetInput,
    MetricTreeSelector,
    MonteCarloInput,
    ScenarioOverride,
    ScoringPolicySelector,
    SensitivityConfig,
)


class MetricTreeTemplateCreate(BaseModel):
    template_name: str = Field(min_length=1, max_length=255)
    version: str = Field(min_length=1, max_length=32)
    definition: dict[str, Any]
    is_default: bool = False


class MetricTreeTemplateRecord(BaseModel):
    id: str
    template_name: str
    version: str
    definition: dict[str, Any]
    is_default: bool
    created_by: str
    created_at: datetime


class MetricTreeTemplateList(BaseModel):
    items: list[MetricTreeTemplateRecord]


class MetricTreeGraphNode(BaseModel):
    node_id: str = Field(min_length=1, max_length=128)
    label: str = Field(min_length=1, max_length=255)
    formula: str | None = None
    is_targetable: bool = False
    unit: str | None = Field(default=None, max_length=64)
    description: str | None = Field(default=None, max_length=1024)


class MetricTreeGraphEdge(BaseModel):
    from_node: str = Field(min_length=1, max_length=128)
    to_node: str = Field(min_length=1, max_length=128)


class MetricTreeGraphPayload(BaseModel):
    nodes: list[MetricTreeGraphNode] = Field(min_length=1)
    edges: list[MetricTreeGraphEdge] = Field(default_factory=list)


class MetricTreeGraphCreate(BaseModel):
    template_name: str = Field(min_length=1, max_length=255)
    version: str = Field(min_length=1, max_length=32)
    graph: MetricTreeGraphPayload
    is_default: bool = False


class MetricTreeGraphRecord(BaseModel):
    id: str
    template_name: str
    version: str
    graph: MetricTreeGraphPayload
    is_default: bool
    is_legacy: bool = False
    created_by: str
    created_at: datetime


class MetricTreeGraphList(BaseModel):
    items: list[MetricTreeGraphRecord]


class MetricTreeGraphVersionEntry(BaseModel):
    version: str
    created_at: datetime
    is_default: bool
    is_legacy: bool = False


class MetricTreeGraphVersionList(BaseModel):
    template_name: str
    items: list[MetricTreeGraphVersionEntry]


class MetricTreeGraphValidationResponse(BaseModel):
    valid: bool
    errors: list[str]
    warnings: list[str]
    stats: dict[str, int]


class EvidencePriorEntry(BaseModel):
    evidence_type: str
    default_confidence: float = Field(ge=0.0, le=1.0)
    default_uplift_sd: float = Field(ge=0.0)
    default_dist_type: str


class EvidencePriorsSetCreate(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    version: str = Field(min_length=1, max_length=32)
    priors: list[EvidencePriorEntry]
    is_default: bool = False


class EvidencePriorsSetRecord(BaseModel):
    id: str
    name: str
    version: str
    priors: list[EvidencePriorEntry]
    is_default: bool
    created_by: str
    created_at: datetime


class EvidencePriorsSetList(BaseModel):
    items: list[EvidencePriorsSetRecord]


class ScoringPolicyCreate(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    version: str = Field(min_length=1, max_length=32)
    policy: dict[str, Any]
    is_default: bool = False


class ScoringPolicyRecord(BaseModel):
    id: str
    name: str
    version: str
    policy: dict[str, Any]
    is_default: bool
    created_by: str
    created_at: datetime


class ScoringPolicyList(BaseModel):
    items: list[ScoringPolicyRecord]


class ConfigDimensionCreate(BaseModel):
    slug: str = Field(min_length=1, max_length=128)
    name: str = Field(min_length=1, max_length=255)
    description: str | None = Field(default=None, max_length=4096)
    is_active: bool = True


class ConfigDimensionUpdate(BaseModel):
    slug: str | None = Field(default=None, min_length=1, max_length=128)
    name: str | None = Field(default=None, min_length=1, max_length=255)
    description: str | None = Field(default=None, max_length=4096)
    is_active: bool | None = None


class ConfigDimensionRecord(BaseModel):
    id: str
    slug: str
    name: str
    description: str | None
    is_active: bool
    created_at: datetime
    updated_at: datetime


class ConfigDimensionList(BaseModel):
    items: list[ConfigDimensionRecord]


MetricKind = str


class ConfigMetricCreate(BaseModel):
    slug: str = Field(min_length=1, max_length=128)
    name: str = Field(min_length=1, max_length=255)
    kind: str = Field(min_length=1, max_length=32)
    driver_key: str = Field(min_length=1, max_length=255)
    unit: str | None = Field(default=None, max_length=64)
    description: str | None = Field(default=None, max_length=4096)
    is_active: bool = True


class ConfigMetricUpdate(BaseModel):
    slug: str | None = Field(default=None, min_length=1, max_length=128)
    name: str | None = Field(default=None, min_length=1, max_length=255)
    kind: str | None = Field(default=None, min_length=1, max_length=32)
    driver_key: str | None = Field(default=None, min_length=1, max_length=255)
    unit: str | None = Field(default=None, max_length=64)
    description: str | None = Field(default=None, max_length=4096)
    is_active: bool | None = None


class ConfigMetricRecord(BaseModel):
    id: str
    slug: str
    name: str
    kind: str
    driver_key: str
    unit: str | None
    description: str | None
    is_active: bool
    created_at: datetime
    updated_at: datetime


class ConfigMetricList(BaseModel):
    items: list[ConfigMetricRecord]


class ScopeSourceCounts(BaseModel):
    datasets: int = 0
    ab_results: int = 0
    initiative_versions: int = 0


class ScopeRecord(BaseModel):
    id: str
    label: str
    kind: str
    is_default: bool
    is_legacy: bool
    read_only: bool
    source_counts: ScopeSourceCounts
    last_seen_at: datetime | None


class ScopeListResponse(BaseModel):
    items: list[ScopeRecord]


class AssumptionsJsonShape(BaseModel):
    data_scope: str | None = None
    baseline_window: str = Field(default="quarter")
    baseline_date_start: str | None = None
    baseline_date_end: str | None = None
    p_success: float = Field(ge=0.0, le=1.0)
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    evidence_type: str | None = None
    effort_cost: float = Field(gt=0)
    strategic_weight: float = Field(ge=0.0)
    learning_value: float = Field(ge=0.0)
    horizon_weeks: int = Field(ge=1, le=520)
    horizons_weeks: list[int] | None = None
    decay: DecayConfig | None = None
    discount_rate_annual: float | None = Field(default=None, ge=0.0)
    cannibalization: CannibalizationInput
    interactions: list[InteractionInput]
    monte_carlo: MonteCarloInput
    scenarios: dict[str, ScenarioOverride] | None = None
    sensitivity: SensitivityConfig
    learning: LearningConfig | None = None
    input_versions: InputVersions | None = None
    metric_tree: MetricTreeSelector | None = None
    scoring_policy: ScoringPolicySelector | None = None


class JsonSchemasResponse(BaseModel):
    schema_version: str
    metric_targets_json: dict[str, Any]
    assumptions_json: dict[str, Any]
    screens_json: dict[str, Any]
    segments_json: dict[str, Any]
    cannibalization_json: dict[str, Any]
    interactions_json: dict[str, Any]
    dataset_schemas: dict[str, Any]
    conventions: dict[str, Any]


class ScoringMethodologyResponse(BaseModel):
    version: str
    canonical_metrics: dict[str, Any]
    baseline_model: dict[str, Any]
    monthly_baseline_aggregation: dict[str, Any]
    causal_chain: list[dict[str, Any]]
    screen_uplift_semantics: dict[str, Any]
    per_screen_breakdown: dict[str, Any]
    driver_effects: list[dict[str, Any]]
    physical_vs_expected: dict[str, Any]
    probability_and_confidence: dict[str, Any]
    learning: dict[str, Any]
    cannibalization: dict[str, Any]
    scenarios: dict[str, Any]
    horizons: dict[str, Any]
    analytics_layers: dict[str, Any]
    monte_carlo: dict[str, Any]
    examples: list[dict[str, Any]]
