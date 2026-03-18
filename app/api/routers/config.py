from fastapi import APIRouter, Depends, Query
from fastapi.responses import PlainTextResponse
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.schemas.config import (
    ConfigMetricCreate,
    ConfigMetricList,
    ConfigMetricRecord,
    ConfigMetricUpdate,
    ConfigDimensionCreate,
    ConfigDimensionList,
    ConfigDimensionRecord,
    ConfigDimensionUpdate,
    EvidencePriorsSetCreate,
    EvidencePriorsSetList,
    EvidencePriorsSetRecord,
    JsonSchemasResponse,
    ScoringMethodologyResponse,
    MetricTreeGraphCreate,
    MetricTreeGraphList,
    MetricTreeGraphPayload,
    MetricTreeGraphRecord,
    MetricTreeGraphValidationResponse,
    MetricTreeGraphVersionEntry,
    MetricTreeGraphVersionList,
    MetricTreeTemplateCreate,
    MetricTreeTemplateList,
    MetricTreeTemplateRecord,
    ScopeListResponse,
    ScopeRecord,
    ScopeSourceCounts,
    ScoringPolicyCreate,
    ScoringPolicyList,
    ScoringPolicyRecord,
)
from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.core.security import Principal, get_current_principal, require_admin
from app.db.models import ConfigMetric, ConfigScreen, ConfigSegment, EvidencePriorsSet, MetricTreeGraph, MetricTreeTemplate, ScoringPolicy
from app.db.repositories import configs as config_repo
from app.db.session import get_session
from app.services.json_schema_docs import build_json_schemas_doc, render_json_schemas_text
from app.services.metric_tree_graph import validate_metric_tree_graph
from app.services.scoring_methodology import build_scoring_methodology, render_scoring_methodology_text

router = APIRouter(prefix="/config", tags=["config"])


def _to_dimension_record(model: ConfigScreen | ConfigSegment) -> ConfigDimensionRecord:
    return ConfigDimensionRecord(
        id=model.id,
        slug=model.slug,
        name=model.name,
        description=model.description,
        is_active=model.is_active,
        created_at=model.created_at,
        updated_at=model.updated_at,
    )


def _to_metric_record(model: ConfigMetric) -> ConfigMetricRecord:
    return ConfigMetricRecord(
        id=model.id,
        slug=model.slug,
        name=model.name,
        kind=model.kind,
        driver_key=model.driver_key,
        unit=model.unit,
        description=model.description,
        is_active=model.is_active,
        created_at=model.created_at,
        updated_at=model.updated_at,
    )


def _scope_metadata(scope_id: str) -> tuple[str, str, bool, bool]:
    if scope_id == "prod":
        return "Production", "prod", False, False
    if scope_id == "x5_retail_test_v2":
        return "X5 Retail Test v2", "test", False, False
    if scope_id == "x5_retail_test_v1":
        return "X5 Retail Test v1", "test", True, True
    if scope_id.startswith("x5_validation_"):
        return scope_id, "validation", False, False
    return scope_id, "custom", False, False


def _scope_sort_key(item: ScopeRecord) -> tuple[int, float, str]:
    if item.id == "prod":
        return (0, 0.0, item.id)
    if item.id == "x5_retail_test_v2":
        return (1, 0.0, item.id)
    if item.id == "x5_retail_test_v1":
        return (2, 0.0, item.id)
    if item.kind == "validation":
        timestamp = item.last_seen_at.timestamp() if item.last_seen_at else 0.0
        return (3, -timestamp, item.id)
    return (4, 0.0, item.id)


def _to_scope_record(scope_id: str, raw: dict[str, object]) -> ScopeRecord:
    label, kind, is_legacy, read_only = _scope_metadata(scope_id)
    return ScopeRecord(
        id=scope_id,
        label=label,
        kind=kind,
        is_default=scope_id == "prod",
        is_legacy=is_legacy,
        read_only=read_only,
        source_counts=ScopeSourceCounts(
            datasets=int(raw.get("datasets", 0) or 0),
            ab_results=int(raw.get("ab_results", 0) or 0),
            initiative_versions=int(raw.get("initiative_versions", 0) or 0),
        ),
        last_seen_at=raw.get("last_seen_at"),
    )


@router.get("/scopes", response_model=ScopeListResponse)
async def list_scopes(
    _: Principal = Depends(get_current_principal),
    session: AsyncSession = Depends(get_session),
) -> ScopeListResponse:
    usage = await config_repo.get_scope_usage_summary(session)
    usage.setdefault(
        "prod",
        {
            "datasets": 0,
            "ab_results": 0,
            "initiative_versions": 0,
            "last_seen_at": None,
        },
    )
    items = [_to_scope_record(scope_id, raw) for scope_id, raw in usage.items()]
    items.sort(key=_scope_sort_key)
    return ScopeListResponse(items=items)


@router.get("/metric-trees", response_model=MetricTreeTemplateList)
async def get_metric_trees(
    _: Principal = Depends(get_current_principal),
    session: AsyncSession = Depends(get_session),
) -> MetricTreeTemplateList:
    records = await config_repo.list_metric_trees(session)
    items = [
        MetricTreeTemplateRecord(
            id=record.id,
            template_name=record.template_name,
            version=record.version,
            definition=record.definition_json,
            is_default=record.is_default,
            created_by=record.created_by,
            created_at=record.created_at,
        )
        for record in records
    ]
    return MetricTreeTemplateList(items=items)


def _to_metric_tree_graph_record(model: MetricTreeGraph) -> MetricTreeGraphRecord:
    return MetricTreeGraphRecord(
        id=model.id,
        template_name=model.template_name,
        version=model.version,
        graph=model.graph_json,
        is_default=model.is_default,
        is_legacy=_is_metric_tree_graph_legacy(model.template_name, model.version),
        created_by=model.created_by,
        created_at=model.created_at,
    )


def _is_metric_tree_graph_legacy(template_name: str, version: str) -> bool:
    return template_name == "x5_retail_test_tree" and version in {"v1", "v2"}


@router.get("/metric-tree-graphs", response_model=MetricTreeGraphList)
async def list_metric_tree_graphs(
    template_name: str | None = Query(default=None),
    version: str | None = Query(default=None),
    _: Principal = Depends(get_current_principal),
    session: AsyncSession = Depends(get_session),
) -> MetricTreeGraphList:
    rows = await config_repo.list_metric_tree_graphs(
        session,
        template_name=template_name,
        version=version,
    )
    return MetricTreeGraphList(items=[_to_metric_tree_graph_record(row) for row in rows])


@router.post("/metric-tree-graphs", response_model=MetricTreeGraphRecord)
async def create_metric_tree_graph(
    body: MetricTreeGraphCreate,
    principal: Principal = Depends(require_admin),
    session: AsyncSession = Depends(get_session),
) -> MetricTreeGraphRecord:
    errors, _warnings = validate_metric_tree_graph(body.graph)
    if errors:
        raise ValidationError("; ".join(errors))

    if body.is_default:
        await config_repo.clear_metric_tree_graph_default(session, template_name=body.template_name)

    model = MetricTreeGraph(
        template_name=body.template_name,
        version=body.version,
        graph_json=body.graph.model_dump(mode="json"),
        is_default=body.is_default,
        created_by=principal.sub,
    )
    session.add(model)
    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        raise ConflictError(
            f"Metric tree graph `{body.template_name}` version `{body.version}` already exists"
        ) from exc
    await session.refresh(model)
    return _to_metric_tree_graph_record(model)


@router.get("/metric-tree-graphs/{template_name}/versions", response_model=MetricTreeGraphVersionList)
async def list_metric_tree_graph_versions(
    template_name: str,
    _: Principal = Depends(get_current_principal),
    session: AsyncSession = Depends(get_session),
) -> MetricTreeGraphVersionList:
    rows = await config_repo.list_metric_tree_graph_versions(session, template_name=template_name)
    if not rows:
        raise NotFoundError(f"Metric tree graph `{template_name}` not found")
    return MetricTreeGraphVersionList(
        template_name=template_name,
        items=[
            MetricTreeGraphVersionEntry(
                version=row.version,
                created_at=row.created_at,
                is_default=row.is_default,
                is_legacy=_is_metric_tree_graph_legacy(template_name, row.version),
            )
            for row in rows
        ],
    )


@router.get("/metric-tree-graphs/{template_name}/{version}", response_model=MetricTreeGraphRecord)
async def get_metric_tree_graph(
    template_name: str,
    version: str,
    _: Principal = Depends(get_current_principal),
    session: AsyncSession = Depends(get_session),
) -> MetricTreeGraphRecord:
    model = await config_repo.get_metric_tree_graph(
        session,
        template_name=template_name,
        version=version,
    )
    if not model:
        raise NotFoundError(f"Metric tree graph `{template_name}` version `{version}` not found")
    return _to_metric_tree_graph_record(model)


@router.post("/metric-tree-graphs/{template_name}/{version}/validate", response_model=MetricTreeGraphValidationResponse)
async def validate_metric_tree_graph_endpoint(
    template_name: str,
    version: str,
    _: Principal = Depends(get_current_principal),
    session: AsyncSession = Depends(get_session),
) -> MetricTreeGraphValidationResponse:
    model = await config_repo.get_metric_tree_graph(
        session,
        template_name=template_name,
        version=version,
    )
    if not model:
        raise NotFoundError(f"Metric tree graph `{template_name}` version `{version}` not found")
    payload = model.graph_json
    graph_payload = MetricTreeGraphPayload.model_validate(payload)
    errors, warnings = validate_metric_tree_graph(graph_payload)
    return MetricTreeGraphValidationResponse(
        valid=not errors,
        errors=errors,
        warnings=warnings,
        stats={
            "nodes": len(payload.get("nodes", [])),
            "edges": len(payload.get("edges", [])),
            "targetable_nodes": sum(1 for node in payload.get("nodes", []) if node.get("is_targetable")),
        },
    )


@router.post("/metric-trees", response_model=MetricTreeTemplateRecord)
async def create_metric_tree(
    body: MetricTreeTemplateCreate,
    principal: Principal = Depends(require_admin),
    session: AsyncSession = Depends(get_session),
) -> MetricTreeTemplateRecord:
    if body.is_default:
        await config_repo.clear_metric_tree_default(session, template_name=body.template_name)

    model = MetricTreeTemplate(
        template_name=body.template_name,
        version=body.version,
        definition_json=body.definition,
        is_default=body.is_default,
        created_by=principal.sub,
    )
    session.add(model)
    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        raise ConflictError(
            f"Metric tree `{body.template_name}` version `{body.version}` already exists"
        ) from exc

    await session.refresh(model)
    return MetricTreeTemplateRecord(
        id=model.id,
        template_name=model.template_name,
        version=model.version,
        definition=model.definition_json,
        is_default=model.is_default,
        created_by=model.created_by,
        created_at=model.created_at,
    )


@router.get("/evidence-priors", response_model=EvidencePriorsSetList)
async def get_evidence_priors(
    _: Principal = Depends(get_current_principal),
    session: AsyncSession = Depends(get_session),
) -> EvidencePriorsSetList:
    records = await config_repo.list_evidence_priors_sets(session)
    items = [
        EvidencePriorsSetRecord(
            id=record.id,
            name=record.name,
            version=record.version,
            priors=record.priors_json.get("priors", []),
            is_default=record.is_default,
            created_by=record.created_by,
            created_at=record.created_at,
        )
        for record in records
    ]
    return EvidencePriorsSetList(items=items)


@router.post("/evidence-priors", response_model=EvidencePriorsSetRecord)
async def create_evidence_priors(
    body: EvidencePriorsSetCreate,
    principal: Principal = Depends(require_admin),
    session: AsyncSession = Depends(get_session),
) -> EvidencePriorsSetRecord:
    if body.is_default:
        await config_repo.clear_evidence_priors_default(session, name=body.name)

    model = EvidencePriorsSet(
        name=body.name,
        version=body.version,
        priors_json={"priors": [item.model_dump(mode="json") for item in body.priors]},
        is_default=body.is_default,
        created_by=principal.sub,
    )
    session.add(model)
    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        raise ConflictError(
            f"Evidence priors set `{body.name}` version `{body.version}` already exists"
        ) from exc

    await session.refresh(model)
    return EvidencePriorsSetRecord(
        id=model.id,
        name=model.name,
        version=model.version,
        priors=model.priors_json.get("priors", []),
        is_default=model.is_default,
        created_by=model.created_by,
        created_at=model.created_at,
    )


@router.get("/scoring-policies", response_model=ScoringPolicyList)
async def get_scoring_policies(
    _: Principal = Depends(get_current_principal),
    session: AsyncSession = Depends(get_session),
) -> ScoringPolicyList:
    records = await config_repo.list_scoring_policies(session)
    items = [
        ScoringPolicyRecord(
            id=record.id,
            name=record.name,
            version=record.version,
            policy=record.policy_json,
            is_default=record.is_default,
            created_by=record.created_by,
            created_at=record.created_at,
        )
        for record in records
    ]
    return ScoringPolicyList(items=items)


@router.post("/scoring-policies", response_model=ScoringPolicyRecord)
async def create_scoring_policy(
    body: ScoringPolicyCreate,
    principal: Principal = Depends(require_admin),
    session: AsyncSession = Depends(get_session),
) -> ScoringPolicyRecord:
    if body.is_default:
        await config_repo.clear_scoring_policy_default(session, name=body.name)

    model = ScoringPolicy(
        name=body.name,
        version=body.version,
        policy_json=body.policy,
        is_default=body.is_default,
        created_by=principal.sub,
    )
    session.add(model)
    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        raise ConflictError(
            f"Scoring policy `{body.name}` version `{body.version}` already exists"
        ) from exc

    await session.refresh(model)
    return ScoringPolicyRecord(
        id=model.id,
        name=model.name,
        version=model.version,
        policy=model.policy_json,
        is_default=model.is_default,
        created_by=model.created_by,
        created_at=model.created_at,
    )


@router.get("/screens", response_model=ConfigDimensionList)
async def list_screens(
    active_only: bool | None = Query(default=None),
    _: Principal = Depends(get_current_principal),
    session: AsyncSession = Depends(get_session),
) -> ConfigDimensionList:
    records = await config_repo.list_config_screens(session, active_only=active_only)
    return ConfigDimensionList(items=[_to_dimension_record(item) for item in records])


@router.post("/screens", response_model=ConfigDimensionRecord)
async def create_screen(
    body: ConfigDimensionCreate,
    _: Principal = Depends(require_admin),
    session: AsyncSession = Depends(get_session),
) -> ConfigDimensionRecord:
    try:
        model = await config_repo.create_config_screen(
            session,
            slug=body.slug,
            name=body.name,
            description=body.description,
            is_active=body.is_active,
        )
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        raise ConflictError(f"Screen slug `{body.slug}` already exists") from exc

    await session.refresh(model)
    return _to_dimension_record(model)


@router.patch("/screens/{screen_id}", response_model=ConfigDimensionRecord)
async def patch_screen(
    screen_id: str,
    body: ConfigDimensionUpdate,
    _: Principal = Depends(require_admin),
    session: AsyncSession = Depends(get_session),
) -> ConfigDimensionRecord:
    model = await config_repo.get_config_screen(session, screen_id)
    if not model:
        raise NotFoundError(f"Screen `{screen_id}` not found")

    update_data = body.model_dump(exclude_unset=True)
    if not update_data:
        raise ValidationError("At least one field must be provided")

    for key, value in update_data.items():
        setattr(model, key, value)

    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        raise ConflictError("Unable to update screen due to unique constraint") from exc

    await session.refresh(model)
    return _to_dimension_record(model)


@router.delete("/screens/{screen_id}", response_model=ConfigDimensionRecord)
async def delete_screen(
    screen_id: str,
    _: Principal = Depends(require_admin),
    session: AsyncSession = Depends(get_session),
) -> ConfigDimensionRecord:
    model = await config_repo.get_config_screen(session, screen_id)
    if not model:
        raise NotFoundError(f"Screen `{screen_id}` not found")
    model.is_active = False
    await session.commit()
    await session.refresh(model)
    return _to_dimension_record(model)


@router.get("/segments", response_model=ConfigDimensionList)
async def list_segments(
    active_only: bool | None = Query(default=None),
    _: Principal = Depends(get_current_principal),
    session: AsyncSession = Depends(get_session),
) -> ConfigDimensionList:
    records = await config_repo.list_config_segments(session, active_only=active_only)
    return ConfigDimensionList(items=[_to_dimension_record(item) for item in records])


@router.post("/segments", response_model=ConfigDimensionRecord)
async def create_segment(
    body: ConfigDimensionCreate,
    _: Principal = Depends(require_admin),
    session: AsyncSession = Depends(get_session),
) -> ConfigDimensionRecord:
    try:
        model = await config_repo.create_config_segment(
            session,
            slug=body.slug,
            name=body.name,
            description=body.description,
            is_active=body.is_active,
        )
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        raise ConflictError(f"Segment slug `{body.slug}` already exists") from exc

    await session.refresh(model)
    return _to_dimension_record(model)


@router.patch("/segments/{segment_id}", response_model=ConfigDimensionRecord)
async def patch_segment(
    segment_id: str,
    body: ConfigDimensionUpdate,
    _: Principal = Depends(require_admin),
    session: AsyncSession = Depends(get_session),
) -> ConfigDimensionRecord:
    model = await config_repo.get_config_segment(session, segment_id)
    if not model:
        raise NotFoundError(f"Segment `{segment_id}` not found")

    update_data = body.model_dump(exclude_unset=True)
    if not update_data:
        raise ValidationError("At least one field must be provided")

    for key, value in update_data.items():
        setattr(model, key, value)

    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        raise ConflictError("Unable to update segment due to unique constraint") from exc

    await session.refresh(model)
    return _to_dimension_record(model)


@router.delete("/segments/{segment_id}", response_model=ConfigDimensionRecord)
async def delete_segment(
    segment_id: str,
    _: Principal = Depends(require_admin),
    session: AsyncSession = Depends(get_session),
) -> ConfigDimensionRecord:
    model = await config_repo.get_config_segment(session, segment_id)
    if not model:
        raise NotFoundError(f"Segment `{segment_id}` not found")
    model.is_active = False
    await session.commit()
    await session.refresh(model)
    return _to_dimension_record(model)


@router.get("/metrics", response_model=ConfigMetricList)
async def list_metrics(
    active_only: bool | None = Query(default=None),
    _: Principal = Depends(get_current_principal),
    session: AsyncSession = Depends(get_session),
) -> ConfigMetricList:
    records = await config_repo.list_config_metrics(session, active_only=active_only)
    return ConfigMetricList(items=[_to_metric_record(item) for item in records])


@router.post("/metrics", response_model=ConfigMetricRecord)
async def create_metric(
    body: ConfigMetricCreate,
    _: Principal = Depends(require_admin),
    session: AsyncSession = Depends(get_session),
) -> ConfigMetricRecord:
    try:
        model = await config_repo.create_config_metric(
            session,
            slug=body.slug,
            name=body.name,
            kind=body.kind,
            driver_key=body.driver_key,
            unit=body.unit,
            description=body.description,
            is_active=body.is_active,
        )
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        raise ConflictError(f"Metric slug `{body.slug}` already exists") from exc

    await session.refresh(model)
    return _to_metric_record(model)


@router.patch("/metrics/{metric_id}", response_model=ConfigMetricRecord)
async def patch_metric(
    metric_id: str,
    body: ConfigMetricUpdate,
    _: Principal = Depends(require_admin),
    session: AsyncSession = Depends(get_session),
) -> ConfigMetricRecord:
    model = await config_repo.get_config_metric(session, metric_id)
    if not model:
        raise NotFoundError(f"Metric `{metric_id}` not found")

    update_data = body.model_dump(exclude_unset=True)
    if not update_data:
        raise ValidationError("At least one field must be provided")

    for key, value in update_data.items():
        setattr(model, key, value)

    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        raise ConflictError("Unable to update metric due to unique constraint") from exc

    await session.refresh(model)
    return _to_metric_record(model)


@router.delete("/metrics/{metric_id}", response_model=ConfigMetricRecord)
async def delete_metric(
    metric_id: str,
    _: Principal = Depends(require_admin),
    session: AsyncSession = Depends(get_session),
) -> ConfigMetricRecord:
    model = await config_repo.get_config_metric(session, metric_id)
    if not model:
        raise NotFoundError(f"Metric `{metric_id}` not found")
    model.is_active = False
    await session.commit()
    await session.refresh(model)
    return _to_metric_record(model)


@router.get("/json-schemas", response_model=JsonSchemasResponse)
async def get_json_schemas(
    _: Principal = Depends(get_current_principal),
) -> JsonSchemasResponse:
    return build_json_schemas_doc()


@router.get("/json-schemas/text", response_class=PlainTextResponse)
async def get_json_schemas_text(
    _: Principal = Depends(get_current_principal),
) -> str:
    doc = build_json_schemas_doc()
    return render_json_schemas_text(doc)


@router.get("/scoring-methodology", response_model=ScoringMethodologyResponse)
async def get_scoring_methodology(
    _: Principal = Depends(get_current_principal),
) -> ScoringMethodologyResponse:
    return ScoringMethodologyResponse.model_validate(build_scoring_methodology())


@router.get("/scoring-methodology/text", response_class=PlainTextResponse)
async def get_scoring_methodology_text(
    _: Principal = Depends(get_current_principal),
) -> str:
    return render_scoring_methodology_text(build_scoring_methodology())
