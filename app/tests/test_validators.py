import pyarrow as pa
import pytest

from app.core.errors import ValidationError
from app.services.validators import validate_dataset_table


def test_baseline_metrics_monthly_schema_exact_columns_pass() -> None:
    table = pa.table(
        {
            "segment_id": ["s1"],
            "date_start": ["2025-01-01"],
            "date_end": ["2025-01-31"],
            "active_users": [1000.0],
            "ordering_users": [100.0],
            "orders": [200.0],
            "items": [250.0],
            "rto": [5000.0],
            "fm": [1500.0],
        }
    )
    validate_dataset_table("baseline_metrics", table)


def test_baseline_metrics_rejects_extra_column() -> None:
    table = pa.table(
        {
            "segment_id": ["s1"],
            "date_start": ["2025-01-01"],
            "date_end": ["2025-01-31"],
            "active_users": [1000.0],
            "ordering_users": [100.0],
            "orders": [200.0],
            "items": [250.0],
            "rto": [5000.0],
            "fm": [1500.0],
            "extra": [1.0],
        }
    )
    with pytest.raises(ValidationError):
        validate_dataset_table("baseline_metrics", table)


def test_baseline_metrics_rejects_invalid_month_range() -> None:
    table = pa.table(
        {
            "segment_id": ["s1"],
            "date_start": ["2025-01-02"],
            "date_end": ["2025-01-31"],
            "active_users": [1000.0],
            "ordering_users": [100.0],
            "orders": [200.0],
            "items": [250.0],
            "rto": [5000.0],
            "fm": [1500.0],
        }
    )
    with pytest.raises(ValidationError):
        validate_dataset_table("baseline_metrics", table)


def test_baseline_metrics_rejects_invalid_relationships() -> None:
    table = pa.table(
        {
            "segment_id": ["s1"],
            "date_start": ["2025-01-01"],
            "date_end": ["2025-01-31"],
            "active_users": [100.0],
            "ordering_users": [120.0],
            "orders": [200.0],
            "items": [250.0],
            "rto": [5000.0],
            "fm": [1500.0],
        }
    )
    with pytest.raises(ValidationError):
        validate_dataset_table("baseline_metrics", table)


def test_baseline_funnel_steps_rejects_duplicate_keys() -> None:
    table = pa.table(
        {
            "segment_id": ["s1", "s1"],
            "screen": ["home", "home"],
            "step_id": ["catalog_to_cart", "catalog_to_cart"],
            "step_name": ["catalog_to_cart", "catalog_to_cart"],
            "step_order": [1, 1],
            "date_start": ["2025-01-01", "2025-01-01"],
            "date_end": ["2025-01-31", "2025-01-31"],
            "entered_users": [100.0, 100.0],
            "advanced_users": [40.0, 40.0],
        }
    )
    with pytest.raises(ValidationError):
        validate_dataset_table("baseline_funnel_steps", table)


def test_baseline_funnel_steps_rejects_advanced_gt_entered() -> None:
    table = pa.table(
        {
            "segment_id": ["s1"],
            "screen": ["home"],
            "step_id": ["catalog_to_cart"],
            "step_name": ["catalog_to_cart"],
            "step_order": [1],
            "date_start": ["2025-01-01"],
            "date_end": ["2025-01-31"],
            "entered_users": [100.0],
            "advanced_users": [120.0],
        }
    )
    with pytest.raises(ValidationError):
        validate_dataset_table("baseline_funnel_steps", table)
