from sqlalchemy import Select, desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.schemas.initiative import (
    InitiativeCreator,
    InitiativeLatestRunMetrics,
    InitiativeRead,
    InitiativeRunSummary,
    InitiativeVersionRead,
    InitiativeVersionSummary,
)
from app.api.schemas.team import TeamRead
from app.db.models import Initiative, InitiativeVersion, ScoringRun, Team
from app.db.repositories import scoring_runs as scoring_repo


def to_team_read(team: Team) -> TeamRead:
    return TeamRead(
        id=team.id,
        slug=team.slug,
        name=team.name,
        description=team.description,
        is_active=team.is_active,
        created_at=team.created_at,
        updated_at=team.updated_at,
    )


def to_initiative_version_read(version: InitiativeVersion) -> InitiativeVersionRead:
    assumptions = version.assumptions_json or {}
    return InitiativeVersionRead(
        id=version.id,
        initiative_id=version.initiative_id,
        version_number=version.version_number,
        title_override=version.title_override,
        description_override=version.description_override,
        data_scope=version.data_scope or assumptions.get("data_scope", "prod"),
        screens=version.screens_json,
        segments=version.segments_json,
        metric_targets=version.metric_targets_json,
        assumptions_json=assumptions,
        p_success=version.p_success if version.p_success is not None else assumptions.get("p_success", 0.0),
        confidence=version.confidence if version.confidence is not None else assumptions.get("confidence"),
        evidence_type=version.evidence_type or assumptions.get("evidence_type"),
        effort_cost=version.effort_cost if version.effort_cost is not None else assumptions.get("effort_cost", 0.0),
        strategic_weight=version.strategic_weight if version.strategic_weight is not None else assumptions.get("strategic_weight", 1.0),
        learning_value=version.learning_value if version.learning_value is not None else assumptions.get("learning_value", 1.0),
        horizon_weeks=version.horizon_weeks if version.horizon_weeks is not None else assumptions.get("horizon_weeks", 1),
        horizons_weeks=assumptions.get("horizons_weeks"),
        decay=version.decay_json or assumptions.get("decay"),
        discount_rate_annual=(
            version.discount_rate_annual if version.discount_rate_annual is not None else assumptions.get("discount_rate_annual")
        ),
        cannibalization=version.cannibalization_json or assumptions.get("cannibalization") or {"mode": "off"},
        interactions=version.interactions_json or assumptions.get("interactions") or [],
        monte_carlo=assumptions.get("monte_carlo") or {"n": 10000, "seed": 123, "enabled": True},
        scenarios=assumptions.get("scenarios"),
        sensitivity=assumptions.get("sensitivity") or {"enabled": False, "epsilon": 0.1, "top_n": 10, "target_metric": "net_margin"},
        input_versions=assumptions.get("input_versions"),
        metric_tree=assumptions.get("metric_tree"),
        scoring_policy=assumptions.get("scoring_policy"),
        created_by_user_id=version.created_by_user_id,
        created_by_email=version.created_by_email,
        change_comment=version.change_comment,
        created_at=version.created_at,
    )


def _extract_latest_run_metrics(last_run: ScoringRun | None) -> InitiativeLatestRunMetrics | None:
    if not last_run:
        return None
    deterministic = last_run.deterministic_output_json or {}
    probabilistic = last_run.probabilistic_output_json or {}
    return InitiativeLatestRunMetrics(
        expected_gmv=deterministic.get("incremental_gmv", deterministic.get("incremental_rto")),
        expected_margin=deterministic.get("expected_margin", deterministic.get("expected_fm", deterministic.get("incremental_fm", deterministic.get("incremental_margin")))),
        expected_rto=deterministic.get("expected_rto", deterministic.get("incremental_rto")),
        expected_fm=deterministic.get("expected_fm", deterministic.get("expected_margin", deterministic.get("incremental_fm"))),
        roi=deterministic.get("roi"),
        priority_score=deterministic.get("priority_score"),
        prob_negative=probabilistic.get("prob_negative"),
        uncertainty_tag=deterministic.get("uncertainty_tag"),
        run_id=last_run.id,
        run_created_at=last_run.created_at,
    )


async def build_initiative_read(
    session: AsyncSession,
    initiative: Initiative,
    *,
    latest_run_override: ScoringRun | None = None,
) -> InitiativeRead:
    team_stmt: Select[tuple[Team]] = select(Team).where(Team.id == initiative.owner_team_id)
    team = await session.scalar(team_stmt)

    latest_version_stmt: Select[tuple[InitiativeVersion]] = (
        select(InitiativeVersion)
        .where(InitiativeVersion.initiative_id == initiative.id)
        .order_by(desc(InitiativeVersion.version_number), desc(InitiativeVersion.created_at))
        .limit(1)
    )
    latest_version = await session.scalar(latest_version_stmt)

    versions_count_stmt = select(func.count(InitiativeVersion.id)).where(InitiativeVersion.initiative_id == initiative.id)
    versions_count = int(await session.scalar(versions_count_stmt) or 0)

    runs_count_stmt = select(func.count(ScoringRun.id)).where(ScoringRun.initiative_id == initiative.id)
    runs_count = int(await session.scalar(runs_count_stmt) or 0)

    if latest_run_override is not None:
        last_run = latest_run_override
    else:
        last_run_stmt: Select[tuple[ScoringRun]] = (
            select(ScoringRun)
            .where(ScoringRun.initiative_id == initiative.id)
            .order_by(desc(ScoringRun.created_at))
            .limit(1)
        )
        last_run = await session.scalar(last_run_stmt)

    latest_summary = (
        InitiativeVersionSummary(
            id=latest_version.id,
            version_number=latest_version.version_number,
            title_override=latest_version.title_override,
            change_comment=latest_version.change_comment,
            created_at=latest_version.created_at,
        )
        if latest_version
        else None
    )
    last_run_summary = (
        InitiativeRunSummary(
            run_id=last_run.id,
            created_at=last_run.created_at,
            triggered_by_user_id=last_run.triggered_by_user_id,
            triggered_by_email=last_run.triggered_by_email,
            run_purpose=last_run.run_purpose,
        )
        if last_run
        else None
    )

    return InitiativeRead(
        id=initiative.id,
        external_key=initiative.external_key,
        name=initiative.name,
        description=initiative.description,
        status=initiative.status,
        owner_team_id=initiative.owner_team_id,
        owner_team=to_team_read(team) if team else None,
        created_by=InitiativeCreator(user_id=initiative.created_by_user_id, email=initiative.created_by_email),
        tags=initiative.tags_json or {},
        created_at=initiative.created_at,
        updated_at=initiative.updated_at,
        archived_at=initiative.archived_at,
        latest_version_number=latest_version.version_number if latest_version else None,
        versions_count=versions_count,
        last_scored_at=last_run.created_at if last_run else None,
        last_scored_by=(last_run.triggered_by_user_id or last_run.created_by) if last_run else None,
        runs_count=runs_count,
        latest_version_summary=latest_summary,
        last_run_summary=last_run_summary,
        latest_run_metrics=_extract_latest_run_metrics(last_run),
    )


async def build_initiative_reads(session: AsyncSession, initiatives: list[Initiative]) -> list[InitiativeRead]:
    if not initiatives:
        return []

    initiative_ids = [initiative.id for initiative in initiatives]
    latest_runs = await scoring_repo.get_latest_runs_for_initiatives(session, initiative_ids)
    rows: list[InitiativeRead] = []
    for initiative in initiatives:
        row = await build_initiative_read(
            session,
            initiative,
            latest_run_override=latest_runs.get(initiative.id),
        )
        rows.append(row)
    return rows
