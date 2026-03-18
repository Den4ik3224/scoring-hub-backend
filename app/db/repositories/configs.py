from datetime import datetime

from sqlalchemy import Select, desc, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import (
    ABExperimentResult,
    ConfigMetric,
    ConfigScreen,
    ConfigSegment,
    Dataset,
    EvidencePriorsSet,
    InitiativeVersion,
    MetricTreeGraph,
    MetricTreeTemplate,
    ScoringPolicy,
)


async def list_metric_trees(session: AsyncSession) -> list[MetricTreeTemplate]:
    stmt: Select[tuple[MetricTreeTemplate]] = select(MetricTreeTemplate).order_by(
        MetricTreeTemplate.template_name.asc(), desc(MetricTreeTemplate.created_at)
    )
    rows = await session.scalars(stmt)
    return list(rows)


async def list_evidence_priors_sets(session: AsyncSession) -> list[EvidencePriorsSet]:
    stmt: Select[tuple[EvidencePriorsSet]] = select(EvidencePriorsSet).order_by(
        EvidencePriorsSet.name.asc(), desc(EvidencePriorsSet.created_at)
    )
    rows = await session.scalars(stmt)
    return list(rows)


async def clear_metric_tree_default(session: AsyncSession, template_name: str) -> None:
    await session.execute(
        update(MetricTreeTemplate)
        .where(MetricTreeTemplate.template_name == template_name)
        .values(is_default=False)
    )


async def clear_metric_tree_graph_default(session: AsyncSession, template_name: str) -> None:
    await session.execute(
        update(MetricTreeGraph)
        .where(MetricTreeGraph.template_name == template_name)
        .values(is_default=False)
    )


async def clear_evidence_priors_default(session: AsyncSession, name: str) -> None:
    await session.execute(
        update(EvidencePriorsSet)
        .where(EvidencePriorsSet.name == name)
        .values(is_default=False)
    )


async def clear_scoring_policy_default(session: AsyncSession, name: str) -> None:
    await session.execute(
        update(ScoringPolicy)
        .where(ScoringPolicy.name == name)
        .values(is_default=False)
    )


async def get_metric_tree(session: AsyncSession, template_name: str, version: str) -> MetricTreeTemplate | None:
    stmt: Select[tuple[MetricTreeTemplate]] = select(MetricTreeTemplate).where(
        MetricTreeTemplate.template_name == template_name,
        MetricTreeTemplate.version == version,
    )
    return await session.scalar(stmt)


async def get_latest_metric_tree_by_name(session: AsyncSession, template_name: str) -> MetricTreeTemplate | None:
    stmt: Select[tuple[MetricTreeTemplate]] = (
        select(MetricTreeTemplate)
        .where(MetricTreeTemplate.template_name == template_name)
        .order_by(desc(MetricTreeTemplate.created_at), desc(MetricTreeTemplate.version))
        .limit(1)
    )
    return await session.scalar(stmt)


async def get_metric_tree_by_version(session: AsyncSession, version: str) -> MetricTreeTemplate | None:
    stmt: Select[tuple[MetricTreeTemplate]] = (
        select(MetricTreeTemplate)
        .where(MetricTreeTemplate.version == version)
        .order_by(desc(MetricTreeTemplate.created_at))
        .limit(1)
    )
    return await session.scalar(stmt)


async def list_metric_tree_graphs(
    session: AsyncSession,
    *,
    template_name: str | None = None,
    version: str | None = None,
) -> list[MetricTreeGraph]:
    stmt: Select[tuple[MetricTreeGraph]] = select(MetricTreeGraph)
    if template_name:
        stmt = stmt.where(MetricTreeGraph.template_name == template_name)
    if version:
        stmt = stmt.where(MetricTreeGraph.version == version)
    stmt = stmt.order_by(MetricTreeGraph.template_name.asc(), desc(MetricTreeGraph.created_at))
    rows = await session.scalars(stmt)
    return list(rows)


async def get_metric_tree_graph(
    session: AsyncSession,
    *,
    template_name: str,
    version: str,
) -> MetricTreeGraph | None:
    stmt: Select[tuple[MetricTreeGraph]] = select(MetricTreeGraph).where(
        MetricTreeGraph.template_name == template_name,
        MetricTreeGraph.version == version,
    )
    return await session.scalar(stmt)


async def get_metric_tree_graph_by_version(session: AsyncSession, version: str) -> MetricTreeGraph | None:
    stmt: Select[tuple[MetricTreeGraph]] = (
        select(MetricTreeGraph)
        .where(MetricTreeGraph.version == version)
        .order_by(desc(MetricTreeGraph.created_at))
        .limit(1)
    )
    return await session.scalar(stmt)


async def list_metric_tree_graph_versions(
    session: AsyncSession,
    *,
    template_name: str,
) -> list[MetricTreeGraph]:
    stmt: Select[tuple[MetricTreeGraph]] = (
        select(MetricTreeGraph)
        .where(MetricTreeGraph.template_name == template_name)
        .order_by(desc(MetricTreeGraph.created_at), desc(MetricTreeGraph.version))
    )
    rows = await session.scalars(stmt)
    return list(rows)


async def get_default_metric_tree_graph(session: AsyncSession) -> MetricTreeGraph | None:
    stmt: Select[tuple[MetricTreeGraph]] = (
        select(MetricTreeGraph)
        .where(MetricTreeGraph.is_default.is_(True))
        .order_by(desc(MetricTreeGraph.created_at))
        .limit(1)
    )
    return await session.scalar(stmt)


async def get_evidence_priors_set(session: AsyncSession, name: str, version: str) -> EvidencePriorsSet | None:
    stmt: Select[tuple[EvidencePriorsSet]] = select(EvidencePriorsSet).where(
        EvidencePriorsSet.name == name,
        EvidencePriorsSet.version == version,
    )
    return await session.scalar(stmt)


async def get_evidence_priors_set_by_version(session: AsyncSession, version: str) -> EvidencePriorsSet | None:
    stmt: Select[tuple[EvidencePriorsSet]] = (
        select(EvidencePriorsSet)
        .where(EvidencePriorsSet.version == version)
        .order_by(desc(EvidencePriorsSet.created_at))
        .limit(1)
    )
    return await session.scalar(stmt)


async def get_default_metric_tree(session: AsyncSession) -> MetricTreeTemplate | None:
    stmt: Select[tuple[MetricTreeTemplate]] = (
        select(MetricTreeTemplate)
        .where(MetricTreeTemplate.is_default.is_(True))
        .order_by(desc(MetricTreeTemplate.created_at))
        .limit(1)
    )
    return await session.scalar(stmt)


async def get_default_evidence_priors(session: AsyncSession) -> EvidencePriorsSet | None:
    stmt: Select[tuple[EvidencePriorsSet]] = (
        select(EvidencePriorsSet)
        .where(EvidencePriorsSet.is_default.is_(True))
        .order_by(desc(EvidencePriorsSet.created_at))
        .limit(1)
    )
    return await session.scalar(stmt)


async def list_scoring_policies(session: AsyncSession) -> list[ScoringPolicy]:
    stmt: Select[tuple[ScoringPolicy]] = select(ScoringPolicy).order_by(
        ScoringPolicy.name.asc(), desc(ScoringPolicy.created_at)
    )
    rows = await session.scalars(stmt)
    return list(rows)


async def get_scoring_policy(session: AsyncSession, name: str, version: str) -> ScoringPolicy | None:
    stmt: Select[tuple[ScoringPolicy]] = select(ScoringPolicy).where(
        ScoringPolicy.name == name,
        ScoringPolicy.version == version,
    )
    return await session.scalar(stmt)


async def get_scoring_policy_by_version(session: AsyncSession, version: str) -> ScoringPolicy | None:
    stmt: Select[tuple[ScoringPolicy]] = (
        select(ScoringPolicy)
        .where(ScoringPolicy.version == version)
        .order_by(desc(ScoringPolicy.created_at))
        .limit(1)
    )
    return await session.scalar(stmt)


async def get_latest_scoring_policy_by_name(session: AsyncSession, name: str) -> ScoringPolicy | None:
    stmt: Select[tuple[ScoringPolicy]] = (
        select(ScoringPolicy)
        .where(ScoringPolicy.name == name)
        .order_by(desc(ScoringPolicy.created_at), desc(ScoringPolicy.version))
        .limit(1)
    )
    return await session.scalar(stmt)


async def get_default_scoring_policy(session: AsyncSession) -> ScoringPolicy | None:
    stmt: Select[tuple[ScoringPolicy]] = (
        select(ScoringPolicy)
        .where(ScoringPolicy.is_default.is_(True))
        .order_by(desc(ScoringPolicy.created_at))
        .limit(1)
    )
    return await session.scalar(stmt)


async def list_config_screens(session: AsyncSession, active_only: bool | None = None) -> list[ConfigScreen]:
    stmt: Select[tuple[ConfigScreen]] = select(ConfigScreen).order_by(
        desc(ConfigScreen.is_active), ConfigScreen.name.asc()
    )
    if active_only is True:
        stmt = stmt.where(ConfigScreen.is_active.is_(True))
    if active_only is False:
        stmt = stmt.where(ConfigScreen.is_active.is_(False))
    rows = await session.scalars(stmt)
    return list(rows)


async def get_config_screen(session: AsyncSession, screen_id: str) -> ConfigScreen | None:
    stmt: Select[tuple[ConfigScreen]] = select(ConfigScreen).where(ConfigScreen.id == screen_id)
    return await session.scalar(stmt)


async def create_config_screen(
    session: AsyncSession,
    *,
    slug: str,
    name: str,
    description: str | None,
    is_active: bool,
) -> ConfigScreen:
    model = ConfigScreen(slug=slug, name=name, description=description, is_active=is_active)
    session.add(model)
    await session.flush()
    return model


async def list_config_segments(session: AsyncSession, active_only: bool | None = None) -> list[ConfigSegment]:
    stmt: Select[tuple[ConfigSegment]] = select(ConfigSegment).order_by(
        desc(ConfigSegment.is_active), ConfigSegment.name.asc()
    )
    if active_only is True:
        stmt = stmt.where(ConfigSegment.is_active.is_(True))
    if active_only is False:
        stmt = stmt.where(ConfigSegment.is_active.is_(False))
    rows = await session.scalars(stmt)
    return list(rows)


async def get_config_segment(session: AsyncSession, segment_id: str) -> ConfigSegment | None:
    stmt: Select[tuple[ConfigSegment]] = select(ConfigSegment).where(ConfigSegment.id == segment_id)
    return await session.scalar(stmt)


async def create_config_segment(
    session: AsyncSession,
    *,
    slug: str,
    name: str,
    description: str | None,
    is_active: bool,
) -> ConfigSegment:
    model = ConfigSegment(slug=slug, name=name, description=description, is_active=is_active)
    session.add(model)
    await session.flush()
    return model


async def list_config_metrics(session: AsyncSession, active_only: bool | None = None) -> list[ConfigMetric]:
    stmt: Select[tuple[ConfigMetric]] = select(ConfigMetric).order_by(
        desc(ConfigMetric.is_active), ConfigMetric.name.asc()
    )
    if active_only is True:
        stmt = stmt.where(ConfigMetric.is_active.is_(True))
    if active_only is False:
        stmt = stmt.where(ConfigMetric.is_active.is_(False))
    rows = await session.scalars(stmt)
    return list(rows)


async def get_scope_usage_summary(
    session: AsyncSession,
) -> dict[str, dict[str, int | datetime | None]]:
    dataset_rows = (
        await session.execute(
            select(
                Dataset.scope.label("scope"),
                func.count(Dataset.id).label("count"),
                func.max(Dataset.created_at).label("last_seen_at"),
            ).group_by(Dataset.scope)
        )
    ).all()
    ab_rows = (
        await session.execute(
            select(
                ABExperimentResult.scope.label("scope"),
                func.count(ABExperimentResult.id).label("count"),
                func.max(ABExperimentResult.created_at).label("last_seen_at"),
            ).group_by(ABExperimentResult.scope)
        )
    ).all()
    initiative_rows = (
        await session.execute(
            select(
                InitiativeVersion.data_scope.label("scope"),
                func.count(InitiativeVersion.id).label("count"),
                func.max(InitiativeVersion.created_at).label("last_seen_at"),
            ).group_by(InitiativeVersion.data_scope)
        )
    ).all()

    summary: dict[str, dict[str, int | datetime | None]] = {}

    def _ensure(scope: str) -> dict[str, int | datetime | None]:
        return summary.setdefault(
            scope,
            {
                "datasets": 0,
                "ab_results": 0,
                "initiative_versions": 0,
                "last_seen_at": None,
            },
        )

    def _merge_last_seen(existing: datetime | None, candidate: datetime | None) -> datetime | None:
        if existing is None:
            return candidate
        if candidate is None:
            return existing
        return max(existing, candidate)

    for row in dataset_rows:
        item = _ensure(row.scope)
        item["datasets"] = int(row.count or 0)
        item["last_seen_at"] = _merge_last_seen(item["last_seen_at"], row.last_seen_at)

    for row in ab_rows:
        item = _ensure(row.scope)
        item["ab_results"] = int(row.count or 0)
        item["last_seen_at"] = _merge_last_seen(item["last_seen_at"], row.last_seen_at)

    for row in initiative_rows:
        item = _ensure(row.scope)
        item["initiative_versions"] = int(row.count or 0)
        item["last_seen_at"] = _merge_last_seen(item["last_seen_at"], row.last_seen_at)

    return summary


async def get_config_metric(session: AsyncSession, metric_id: str) -> ConfigMetric | None:
    stmt: Select[tuple[ConfigMetric]] = select(ConfigMetric).where(ConfigMetric.id == metric_id)
    return await session.scalar(stmt)


async def create_config_metric(
    session: AsyncSession,
    *,
    slug: str,
    name: str,
    kind: str,
    driver_key: str,
    unit: str,
    description: str | None,
    is_active: bool,
) -> ConfigMetric:
    model = ConfigMetric(
        slug=slug,
        name=name,
        kind=kind,
        driver_key=driver_key,
        unit=unit,
        description=description,
        is_active=is_active,
    )
    session.add(model)
    await session.flush()
    return model
