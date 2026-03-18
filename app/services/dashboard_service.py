from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone

from sqlalchemy import Select, desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.schemas.dashboard import (
    DashboardImpactByTeamRow,
    DashboardKpiCards,
    DashboardReviewQueue,
    DashboardStatusCount,
    DashboardSummaryResponse,
    DashboardTopInitiative,
    DashboardUncertaintyCount,
)
from app.db.models import Initiative, ScoringRun, Team


def _to_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


async def build_dashboard_summary(session: AsyncSession) -> DashboardSummaryResponse:
    initiatives_total_stmt = select(func.count(Initiative.id))
    initiatives_active_stmt = select(func.count(Initiative.id)).where(Initiative.status == "active")
    status_counts_stmt: Select[tuple[str, int]] = (
        select(Initiative.status, func.count(Initiative.id))
        .group_by(Initiative.status)
        .order_by(Initiative.status.asc())
    )

    initiatives_total = int(await session.scalar(initiatives_total_stmt) or 0)
    initiatives_active = int(await session.scalar(initiatives_active_stmt) or 0)
    status_counts_rows = await session.execute(status_counts_stmt)

    latest_runs_ranked = (
        select(
            ScoringRun.id.label("run_id"),
            ScoringRun.initiative_id.label("initiative_id"),
            func.row_number()
            .over(partition_by=ScoringRun.initiative_id, order_by=desc(ScoringRun.created_at))
            .label("rn"),
        )
        .where(ScoringRun.initiative_id.is_not(None), ScoringRun.run_status == "success")
        .subquery()
    )

    latest_runs_stmt = (
        select(ScoringRun, Initiative, Team)
        .join(latest_runs_ranked, latest_runs_ranked.c.run_id == ScoringRun.id)
        .join(Initiative, Initiative.id == ScoringRun.initiative_id)
        .outerjoin(Team, Team.id == Initiative.owner_team_id)
        .where(latest_runs_ranked.c.rn == 1)
    )
    latest_rows = (await session.execute(latest_runs_stmt)).all()

    initiatives_with_runs = len(latest_rows)
    expected_fm_total = 0.0
    expected_rto_total = 0.0
    expected_margin_total = 0.0
    expected_gmv_total = 0.0
    roi_values: list[float] = []

    impact_by_team_map: dict[tuple[str | None, str], dict[str, float | int]] = defaultdict(
        lambda: {
            "expected_fm": 0.0,
            "expected_rto": 0.0,
            "expected_margin": 0.0,
            "expected_gmv": 0.0,
            "initiatives_count": 0,
        }
    )
    uncertainty_map: dict[str, int] = defaultdict(int)
    top_initiatives: list[DashboardTopInitiative] = []

    for run, initiative, team in latest_rows:
        deterministic = run.deterministic_output_json or {}
        expected_fm = _to_float(deterministic.get("expected_fm", deterministic.get("expected_margin", deterministic.get("incremental_fm", deterministic.get("incremental_margin")))))
        expected_rto = _to_float(deterministic.get("expected_rto", deterministic.get("expected_gmv", deterministic.get("incremental_rto", deterministic.get("incremental_gmv")))))
        expected_margin = expected_fm
        expected_gmv = expected_rto
        roi = _to_float(deterministic.get("roi"))
        priority_score = _to_float(deterministic.get("priority_score"))
        uncertainty_tag = deterministic.get("uncertainty_tag")

        expected_fm_total += expected_fm
        expected_rto_total += expected_rto
        expected_margin_total += expected_margin
        expected_gmv_total += expected_gmv
        if roi:
            roi_values.append(roi)
        if isinstance(uncertainty_tag, str) and uncertainty_tag:
            uncertainty_map[uncertainty_tag] += 1

        team_id = team.id if team else None
        team_name = team.name if team else "Unassigned"
        team_entry = impact_by_team_map[(team_id, team_name)]
        team_entry["expected_fm"] = _to_float(team_entry["expected_fm"]) + expected_fm
        team_entry["expected_rto"] = _to_float(team_entry["expected_rto"]) + expected_rto
        team_entry["expected_margin"] = _to_float(team_entry["expected_margin"]) + expected_margin
        team_entry["expected_gmv"] = _to_float(team_entry["expected_gmv"]) + expected_gmv
        team_entry["initiatives_count"] = int(team_entry["initiatives_count"]) + 1

        top_initiatives.append(
            DashboardTopInitiative(
                initiative_id=initiative.id,
                initiative_name=initiative.name,
                team_name=team_name,
                run_id=run.id,
                expected_fm=expected_fm,
                expected_margin=expected_margin,
                roi=roi,
                priority_score=priority_score,
                uncertainty_tag=uncertainty_tag,
            )
        )

    top_initiatives.sort(key=lambda item: item.expected_margin, reverse=True)

    impact_by_team = [
        DashboardImpactByTeamRow(
            team_id=team_id,
            team_name=team_name,
            expected_fm=_to_float(values["expected_fm"]),
            expected_rto=_to_float(values["expected_rto"]),
            expected_margin=_to_float(values["expected_margin"]),
            expected_gmv=_to_float(values["expected_gmv"]),
            initiatives_count=int(values["initiatives_count"]),
        )
        for (team_id, team_name), values in sorted(
            impact_by_team_map.items(),
            key=lambda item: _to_float(item[1]["expected_margin"]),
            reverse=True,
        )
    ]

    initiatives_by_status = [
        DashboardStatusCount(status=status, count=int(count))
        for status, count in status_counts_rows
    ]
    uncertainty_distribution = [
        DashboardUncertaintyCount(uncertainty_tag=tag, count=count)
        for tag, count in sorted(uncertainty_map.items(), key=lambda item: item[0])
    ]

    avg_roi = (sum(roi_values) / len(roi_values)) if roi_values else 0.0
    return DashboardSummaryResponse(
        kpi_cards=DashboardKpiCards(
            initiatives_total=initiatives_total,
            initiatives_active=initiatives_active,
            initiatives_with_runs=initiatives_with_runs,
            expected_fm_total=expected_fm_total,
            expected_rto_total=expected_rto_total,
            expected_margin_total=expected_margin_total,
            expected_gmv_total=expected_gmv_total,
            avg_roi=avg_roi,
        ),
        impact_by_team=impact_by_team,
        initiatives_by_status=initiatives_by_status,
        uncertainty_distribution=uncertainty_distribution,
        top_initiatives=top_initiatives[:10],
        review_queue_counts=DashboardReviewQueue(
            available=False,
            reason="review_workflow_source_unavailable",
        ),
        updated_at=datetime.now(timezone.utc),
    )
