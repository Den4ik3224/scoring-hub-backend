import json

from pydantic import TypeAdapter

from app.api.schemas.config import AssumptionsJsonShape, JsonSchemasResponse
from app.api.schemas.score import CannibalizationInput, InteractionInput, MetricTargetInput, SegmentInput

JSON_SCHEMAS_DOC_VERSION = "v2"


def build_json_schemas_doc() -> JsonSchemasResponse:
    return JsonSchemasResponse(
        schema_version=JSON_SCHEMAS_DOC_VERSION,
        metric_targets_json=TypeAdapter(list[MetricTargetInput]).json_schema(),
        assumptions_json=AssumptionsJsonShape.model_json_schema(),
        screens_json=TypeAdapter(list[str]).json_schema(),
        segments_json=TypeAdapter(list[SegmentInput]).json_schema(),
        cannibalization_json=CannibalizationInput.model_json_schema(),
        interactions_json=TypeAdapter(list[InteractionInput]).json_schema(),
        dataset_schemas={
            "baseline_metrics": {
                "description": "Monthly segment-level baseline facts. Screen is not part of this dataset.",
                "grain": ["segment_id", "month"],
                "columns": [
                    {"name": "segment_id", "type": "string", "required": True},
                    {"name": "date_start", "type": "date", "required": True, "rule": "first day of calendar month"},
                    {"name": "date_end", "type": "date", "required": True, "rule": "last day of same calendar month"},
                    {"name": "active_users", "type": "number", "required": True, "min": 0.0},
                    {"name": "ordering_users", "type": "number", "required": True, "min": 0.0},
                    {"name": "orders", "type": "number", "required": True, "min": 0.0},
                    {"name": "items", "type": "number", "required": True, "min": 0.0},
                    {"name": "rto", "type": "number", "required": True, "min": 0.0},
                    {"name": "fm", "type": "number", "required": True, "min": 0.0},
                ],
                "natural_key": ["segment_id", "date_start", "date_end"],
            },
            "baseline_funnel_steps": {
                "description": "Monthly screen-level exposure and transitions within a segment.",
                "grain": ["segment_id", "screen", "step_id", "month"],
                "columns": [
                    {"name": "segment_id", "type": "string", "required": True},
                    {"name": "screen", "type": "string", "required": True},
                    {"name": "step_id", "type": "string", "required": True},
                    {"name": "step_name", "type": "string", "required": True},
                    {"name": "step_order", "type": "integer", "required": True, "min": 1},
                    {"name": "date_start", "type": "date", "required": True, "rule": "first day of calendar month"},
                    {"name": "date_end", "type": "date", "required": True, "rule": "last day of same calendar month"},
                    {"name": "entered_users", "type": "number", "required": True, "min": 0.0},
                    {"name": "advanced_users", "type": "number", "required": True, "min": 0.0},
                ],
                "natural_key": ["segment_id", "screen", "step_id", "date_start", "date_end"],
            },
            "cannibalization_matrix": {
                "description": "Static reallocation matrix between screens.",
                "columns": [
                    {"name": "from_screen", "type": "string", "required": True},
                    {"name": "to_screen", "type": "string", "required": True},
                    {"name": "segment_id", "type": "string", "required": True},
                    {"name": "cannibalization_rate", "type": "number", "required": True, "min": 0.0, "max": 1.0},
                ],
            },
        },
        conventions={
            "data_scope": {
                "description": "Logical isolation scope for datasets, learning evidence and scoring resolution.",
                "default": "prod",
            },
            "csv_upload": {
                "description": "CSV uploads are autodetected between standard comma CSV and semicolon CSV with decimal comma.",
                "supported_delimiters": [",", ";"],
                "supported_date_inputs": ["YYYY-MM-DD", "YYYY-MM-DD HH:MM:SS", "YYYY-MM-DDTHH:MM:SS"],
                "validation_error_behavior": "Invalid CSV uploads return 422 with an actionable message.",
            },
            "baseline_window": {
                "description": "Historical calibration window for baseline aggregation. Distinct from forecast horizon.",
                "supported": ["month", "quarter", "half_year", "year"],
                "default": "quarter",
                "anchor": "latest complete month available in baseline dataset",
                "runtime_note": "Monthly history is converted into a weekly run rate before forecast horizons are applied.",
            },
            "canonical_metric_tree": {
                "template_name": "x5_retail_test_tree",
                "current_default_version": "v3",
                "primary_metrics": ["rto", "fm"],
                "deprecated_aliases": {
                    "incremental_gmv": "incremental_rto",
                    "incremental_margin": "incremental_fm",
                },
                "formulas": {
                    "mau_effective": "mau * penetration",
                    "orders": "mau_effective * conversion * frequency",
                    "items": "orders * aoq",
                    "aov": "aoq * aiv",
                    "rto": "orders * aov",
                    "fm": "rto * fm_pct",
                },
                "frequency_note": "Runtime `frequency` is weekly-normalized from historical `frequency_monthly` using 12/52.",
                "screen_layer": {
                    "role": "screens affect exposure and funnel transitions, not standalone segment economics",
                    "attribution": "per_screen_breakdown is an attributed share of segment delta, not a separate screen P&L",
                },
            },
            "deprecated_aliases": {
                "incremental_gmv": "incremental_rto",
                "incremental_margin": "incremental_fm",
                "expected_gmv": "expected_rto",
                "expected_margin": "expected_fm",
            },
        },
    )


def render_json_schemas_text(doc: JsonSchemasResponse) -> str:
    sections = {
        "metric_targets_json": doc.metric_targets_json,
        "assumptions_json": doc.assumptions_json,
        "screens_json": doc.screens_json,
        "segments_json": doc.segments_json,
        "cannibalization_json": doc.cannibalization_json,
        "interactions_json": doc.interactions_json,
        "dataset_schemas": doc.dataset_schemas,
        "conventions": doc.conventions,
    }
    lines: list[str] = [
        "# Backlog Scoring JSON Shapes",
        "",
        f"schema_version: {doc.schema_version}",
        "",
        "Supported uploadable dataset types:",
        "- baseline_metrics",
        "- baseline_funnel_steps",
        "- cannibalization_matrix",
        "",
        "CSV upload behavior:",
        "- standard comma-delimited CSV is supported",
        "- semicolon-delimited CSV with decimal comma is supported",
        "- invalid CSV uploads return 422 with an actionable validation message",
        "",
        "Baseline model:",
        "- baseline_metrics stores monthly segment economics",
        "- baseline_funnel_steps stores monthly screen exposure and transition facts",
        "- RTO and FM are segment-level outputs; screens do not own standalone economics",
    ]

    for key, value in sections.items():
        lines.append("")
        lines.append(f"## {key}")
        lines.append("```json")
        lines.append(json.dumps(value, ensure_ascii=False, indent=2))
        lines.append("```")

    return "\n".join(lines)
