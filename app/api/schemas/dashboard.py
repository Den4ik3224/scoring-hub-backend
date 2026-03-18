from datetime import datetime

from pydantic import BaseModel


class DashboardKpiCards(BaseModel):
    initiatives_total: int
    initiatives_active: int
    initiatives_with_runs: int
    expected_fm_total: float
    expected_rto_total: float
    expected_margin_total: float
    expected_gmv_total: float
    avg_roi: float


class DashboardImpactByTeamRow(BaseModel):
    team_id: str | None
    team_name: str
    expected_fm: float
    expected_rto: float
    expected_margin: float
    expected_gmv: float
    initiatives_count: int


class DashboardStatusCount(BaseModel):
    status: str
    count: int


class DashboardUncertaintyCount(BaseModel):
    uncertainty_tag: str
    count: int


class DashboardTopInitiative(BaseModel):
    initiative_id: str
    initiative_name: str
    team_name: str
    run_id: str
    expected_fm: float
    expected_margin: float
    roi: float
    priority_score: float
    uncertainty_tag: str | None = None


class DashboardReviewQueue(BaseModel):
    available: bool
    reason: str | None = None
    pending: int | None = None
    approved: int | None = None
    cancelled: int | None = None


class DashboardSummaryResponse(BaseModel):
    kpi_cards: DashboardKpiCards
    impact_by_team: list[DashboardImpactByTeamRow]
    initiatives_by_status: list[DashboardStatusCount]
    uncertainty_distribution: list[DashboardUncertaintyCount]
    top_initiatives: list[DashboardTopInitiative]
    review_queue_counts: DashboardReviewQueue
    updated_at: datetime
