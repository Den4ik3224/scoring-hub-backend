from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

import pyarrow as pa
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.schemas.score import ScoreRunRequest
from app.core.errors import NotFoundError, ValidationError
from app.db.models import Dataset
from app.db.repositories import ab_results as learning_repo
from app.db.repositories import configs as config_repo
from app.db.repositories import datasets as dataset_repo
from app.services.learning_engine import resolve_learning_config, scoring_metric_drivers
from app.services.monthly_baselines import (
    ResolvedBaselineWindow,
    SegmentMonthlyBaseline,
    FunnelStepAggregate,
    aggregate_funnel_steps,
    aggregate_segment_baselines,
    filter_table_to_window,
    resolve_baseline_window,
)
from app.services.scoring_policy import normalize_policy, prepare_metric_targets, prepare_segments


@dataclass
class ResolvedScoringInputs:
    baseline_dataset: Dataset
    baseline_table: pa.Table
    baseline_window: ResolvedBaselineWindow
    segment_baselines: dict[str, SegmentMonthlyBaseline]
    funnel_dataset: Dataset | None = None
    funnel_table: pa.Table | None = None
    funnel_index: dict[tuple[str, str], list[FunnelStepAggregate]] = field(default_factory=dict)
    cannibalization_dataset: Dataset | None = None
    cannibalization_table: pa.Table | None = None
    evidence_priors_source: str | None = None
    evidence_priors: dict[str, dict] | None = None
    metric_tree_source: str | None = None
    metric_tree_definition: dict | None = None
    scoring_policy_source: str = "builtin:ev_policy_vnext_learning:1"
    scoring_policy_snapshot: dict = field(default_factory=dict)
    learning_config: dict = field(default_factory=dict)
    learning_evidence: list[dict] = field(default_factory=list)
    data_scope: str = "prod"
    resolved_versions: dict[str, str] = field(default_factory=dict)
    resolved_inputs_json: dict = field(default_factory=dict)


async def _resolve_dataset(
    session: AsyncSession,
    schema_type: str,
    version: str | None,
    required: bool,
    scope: str,
) -> Dataset | None:
    dataset: Dataset | None
    if version:
        dataset = await dataset_repo.get_dataset_by_schema_type_version(
            session,
            schema_type=schema_type,
            version=version,
            scope=scope,
        )
    else:
        dataset = await dataset_repo.get_latest_dataset_by_schema_type(session, schema_type=schema_type, scope=scope)

    if required and not dataset:
        if version:
            raise NotFoundError(f"Required dataset `{schema_type}` version `{version}` not found")
        raise NotFoundError(f"Required dataset `{schema_type}` not found")

    return dataset


async def _resolve_metric_tree(
    session: AsyncSession,
    payload: ScoreRunRequest,
) -> tuple[str | None, dict | None]:
    if payload.metric_tree:
        selector = payload.metric_tree
        graph = (
            await config_repo.get_metric_tree_graph(session, template_name=selector.template_name, version=selector.version)
            if selector.version
            else None
        )
        if graph is None and not selector.version:
            graphs = await config_repo.list_metric_tree_graphs(session, template_name=selector.template_name)
            graph = graphs[0] if graphs else None
        if graph:
            return f"graph:{graph.template_name}:{graph.version}", graph.graph_json
        raise NotFoundError(
            f"Metric tree `{selector.template_name}`"
            + (f" version `{selector.version}`" if selector.version else "")
            + " not found"
        )

    default_graph = await config_repo.get_default_metric_tree_graph(session)
    if default_graph:
        return f"graph:{default_graph.template_name}:{default_graph.version}", default_graph.graph_json
    default_tree = await config_repo.get_default_metric_tree(session)
    if default_tree:
        return f"config:{default_tree.template_name}:{default_tree.version}", default_tree.definition_json
    return None, None


async def _resolve_evidence_priors(session: AsyncSession) -> tuple[str | None, dict[str, dict] | None]:
    default_priors = await config_repo.get_default_evidence_priors(session)
    if not default_priors:
        return None, None
    priors = {entry["evidence_type"]: entry for entry in default_priors.priors_json.get("priors", [])}
    return f"config:{default_priors.name}:{default_priors.version}", priors


async def resolve_scoring_inputs(
    session: AsyncSession,
    payload: ScoreRunRequest,
) -> ResolvedScoringInputs:
    input_versions = payload.input_versions
    data_scope = payload.data_scope or "prod"

    baseline_dataset = await _resolve_dataset(
        session,
        schema_type="baseline_metrics",
        version=input_versions.baseline_metrics if input_versions else None,
        required=True,
        scope=data_scope,
    )
    assert baseline_dataset is not None
    baseline_rows = await dataset_repo.get_dataset_rows(session, baseline_dataset.id, "baseline_metrics")
    if not baseline_rows:
        raise NotFoundError(f"Dataset rows not found for {baseline_dataset.dataset_name} v{baseline_dataset.version}")
    baseline_table_full = pa.Table.from_pylist(baseline_rows)
    baseline_window = resolve_baseline_window(
        baseline_table_full,
        baseline_window=payload.baseline_window,
        baseline_date_start=payload.baseline_date_start,
        baseline_date_end=payload.baseline_date_end,
    )
    baseline_table = filter_table_to_window(baseline_table_full, baseline_window)
    segment_baselines = aggregate_segment_baselines(
        baseline_table,
        segment_ids=[segment.id for segment in payload.segments],
        window=baseline_window,
    )

    funnel_dataset = await _resolve_dataset(
        session,
        schema_type="baseline_funnel_steps",
        version=input_versions.baseline_funnel_steps if input_versions else None,
        required=False,
        scope=data_scope,
    )
    funnel_table = None
    funnel_index: dict[tuple[str, str], list[FunnelStepAggregate]] = {}
    if funnel_dataset:
        funnel_rows = await dataset_repo.get_dataset_rows(session, funnel_dataset.id, "baseline_funnel_steps")
        if not funnel_rows:
            raise NotFoundError(f"Dataset rows not found for {funnel_dataset.dataset_name} v{funnel_dataset.version}")
        funnel_table_full = pa.Table.from_pylist(funnel_rows)
        funnel_table = filter_table_to_window(funnel_table_full, baseline_window)
        funnel_index = aggregate_funnel_steps(
            funnel_table,
            segment_ids=[segment.id for segment in payload.segments],
            screens=payload.screens,
            window=baseline_window,
        )

    cann_dataset = None
    cann_table = None
    if payload.cannibalization.mode == "matrix":
        cann_version = None
        if input_versions and input_versions.cannibalization_matrix:
            cann_version = input_versions.cannibalization_matrix
        elif payload.cannibalization.matrix_id:
            cann_version = payload.cannibalization.matrix_id
        cann_dataset = await _resolve_dataset(
            session,
            schema_type="cannibalization_matrix",
            version=cann_version,
            required=True,
            scope=data_scope,
        )
        assert cann_dataset is not None
        cann_rows = await dataset_repo.get_dataset_rows(session, cann_dataset.id, "cannibalization_matrix")
        if not cann_rows:
            raise NotFoundError(f"Dataset rows not found for {cann_dataset.dataset_name} v{cann_dataset.version}")
        cann_table = pa.Table.from_pylist(cann_rows)

    evidence_source, evidence_priors = await _resolve_evidence_priors(session)
    metric_tree_source, metric_tree_definition = await _resolve_metric_tree(session, payload)

    if payload.confidence is None and not payload.evidence_type and not evidence_priors:
        raise ValidationError("Either confidence, evidence_type, or configured evidence priors must be provided")

    scoring_policy_source = "builtin:ev_policy_vnext_learning:1"
    scoring_policy_snapshot = {
        "primitive_metrics": ["mau", "penetration", "conversion", "frequency", "frequency_monthly", "aoq", "aiv", "fm_pct"],
        "derived_metrics": ["orders", "items", "aov", "rto", "fm"],
        "translator_enabled": True,
        "translations": {"aov": {"to": ["aoq", "aiv"], "weights": {"aoq": 0.5, "aiv": 0.5}}},
        "default_horizons": [4, 13, 26, 52],
        "learning_defaults": {
            "mode": "bayesian",
            "lookback_days": 730,
            "half_life_days": 180,
            "min_quality": 0.6,
            "min_sample_size": 500,
        },
    }
    if input_versions and input_versions.scoring_policy:
        policy = await config_repo.get_scoring_policy_by_version(session, version=input_versions.scoring_policy)
        if not policy:
            raise NotFoundError(f"Scoring policy version `{input_versions.scoring_policy}` not found")
        scoring_policy_source = f"config:{policy.name}:{policy.version}"
        scoring_policy_snapshot = policy.policy_json
    elif payload.scoring_policy:
        selector = payload.scoring_policy
        policy = (
            await config_repo.get_scoring_policy(session, name=selector.name, version=selector.version)
            if selector.version
            else await config_repo.get_latest_scoring_policy_by_name(session, name=selector.name)
        )
        if not policy:
            raise NotFoundError(
                f"Scoring policy `{selector.name}`"
                + (f" version `{selector.version}`" if selector.version else "")
                + " not found"
            )
        scoring_policy_source = f"config:{policy.name}:{policy.version}"
        scoring_policy_snapshot = policy.policy_json
    else:
        default_policy = await config_repo.get_default_scoring_policy(session)
        if default_policy:
            scoring_policy_source = f"config:{default_policy.name}:{default_policy.version}"
            scoring_policy_snapshot = default_policy.policy_json

    normalized_policy = normalize_policy(
        scoring_policy_snapshot,
        source_name=scoring_policy_source.split(":")[1] if ":" in scoring_policy_source else "builtin_ev_policy",
        source_version=scoring_policy_source.split(":")[-1],
    )

    effective_learning = resolve_learning_config(payload.learning, scoring_policy_snapshot)
    learning_evidence_items: list[dict] = []
    if effective_learning.mode != "off":
        canonical_payload = payload.model_copy(deep=True)
        canonical_payload.segments = prepare_segments(canonical_payload.segments, normalized_policy).segments
        canonical_payload.metric_targets = prepare_metric_targets(canonical_payload.metric_targets, normalized_policy).targets
        metric_drivers = scoring_metric_drivers(canonical_payload)
        lookback_from = datetime.now(timezone.utc) - timedelta(days=effective_learning.lookback_days)
        learning_rows = await learning_repo.list_matching_evidence_for_scoring(
            session,
            scope=data_scope,
            screens=payload.screens,
            metric_drivers=metric_drivers,
            segment_ids=[segment.id for segment in payload.segments],
            min_quality=effective_learning.min_quality,
            min_sample_size=effective_learning.min_sample_size,
            lookback_from=lookback_from,
        )
        learning_evidence_items = [
            {
                "id": row.id,
                "experiment_id": row.experiment_id,
                "initiative_id": row.initiative_id,
                "screen": row.screen,
                "segment_id": row.segment_id,
                "metric_driver": row.metric_driver,
                "observed_uplift": row.observed_uplift,
                "ci_low": row.ci_low,
                "ci_high": row.ci_high,
                "sample_size": row.sample_size,
                "significance_flag": row.significance_flag,
                "quality_score": row.quality_score,
                "source": row.source,
                "start_at": row.start_at.isoformat(),
                "end_at": row.end_at.isoformat(),
                "created_at": row.created_at.isoformat(),
            }
            for row in learning_rows
        ]

    resolved_versions = {"baseline_metrics": baseline_dataset.version}
    if funnel_dataset:
        resolved_versions["baseline_funnel_steps"] = funnel_dataset.version
    if cann_dataset:
        resolved_versions["cannibalization_matrix"] = cann_dataset.version
    if scoring_policy_source:
        resolved_versions["scoring_policy"] = scoring_policy_source.split(":")[-1]

    resolved_inputs_json = {
        "data_scope": data_scope,
        "baseline_window": {
            "name": payload.baseline_window,
            "date_start": baseline_window.start_date.isoformat(),
            "date_end": baseline_window.end_date.isoformat(),
            "anchor_month": baseline_window.anchor_month.isoformat(),
        },
        "datasets": {
            "baseline_metrics": {
                "dataset_name": baseline_dataset.dataset_name,
                "version": baseline_dataset.version,
                "scope": baseline_dataset.scope,
                "checksum": baseline_dataset.checksum_sha256,
            },
        },
        "evidence_priors_source": evidence_source,
        "metric_tree_source": metric_tree_source,
        "scoring_policy_source": scoring_policy_source,
        "scoring_policy_snapshot": scoring_policy_snapshot,
        "learning": {
            "config": effective_learning.model_dump(mode="json"),
            "evidence_count": len(learning_evidence_items),
            "evidence_items": learning_evidence_items,
        },
    }
    if funnel_dataset:
        resolved_inputs_json["datasets"]["baseline_funnel_steps"] = {
            "dataset_name": funnel_dataset.dataset_name,
            "version": funnel_dataset.version,
            "scope": funnel_dataset.scope,
            "checksum": funnel_dataset.checksum_sha256,
        }
    if cann_dataset:
        resolved_inputs_json["datasets"]["cannibalization_matrix"] = {
            "dataset_name": cann_dataset.dataset_name,
            "version": cann_dataset.version,
            "scope": cann_dataset.scope,
            "checksum": cann_dataset.checksum_sha256,
        }

    return ResolvedScoringInputs(
        baseline_dataset=baseline_dataset,
        baseline_table=baseline_table,
        baseline_window=baseline_window,
        segment_baselines=segment_baselines,
        funnel_dataset=funnel_dataset,
        funnel_table=funnel_table,
        funnel_index=funnel_index,
        cannibalization_dataset=cann_dataset,
        cannibalization_table=cann_table,
        evidence_priors_source=evidence_source,
        evidence_priors=evidence_priors,
        metric_tree_source=metric_tree_source,
        metric_tree_definition=metric_tree_definition,
        scoring_policy_source=scoring_policy_source,
        scoring_policy_snapshot=scoring_policy_snapshot,
        learning_config=effective_learning.model_dump(mode="json"),
        learning_evidence=learning_evidence_items,
        data_scope=data_scope,
        resolved_versions=resolved_versions,
        resolved_inputs_json=resolved_inputs_json,
    )
