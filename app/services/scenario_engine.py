from copy import deepcopy

from app.api.schemas.score import ScoreRunRequest
from app.core.errors import ValidationError

ALLOWED_SCENARIO_FIELDS = {
    "segments",
    "metric_targets",
    "p_success",
    "confidence",
    "evidence_type",
    "cannibalization",
    "decay",
}


def materialize_scenarios(payload: ScoreRunRequest) -> dict[str, ScoreRunRequest]:
    base_payload = payload.model_copy(deep=True)
    base_payload.scenarios = None
    scenarios: dict[str, ScoreRunRequest] = {"base": base_payload}

    if not payload.scenarios:
        return scenarios

    for scenario_name, override in payload.scenarios.items():
        if scenario_name in scenarios:
            if scenario_name == "base":
                raise ValidationError(
                    "Scenario `base` is reserved. Configure the base scenario via top-level run fields and use `scenarios` only for non-base overrides."
                )
            raise ValidationError(f"Duplicate scenario name `{scenario_name}`")

        unknown = set(override.model_dump(exclude_none=True).keys()) - ALLOWED_SCENARIO_FIELDS
        if unknown:
            raise ValidationError(
                f"Scenario `{scenario_name}` has unsupported override fields: {sorted(unknown)}"
            )

        scenario_payload = deepcopy(base_payload)
        override_data = override.model_dump(exclude_none=True)
        for key, value in override_data.items():
            setattr(scenario_payload, key, value)
        scenarios[scenario_name] = scenario_payload

    return scenarios
