from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class DatasetFormat(str, Enum):
    csv = "csv"
    parquet = "parquet"


class DatasetSchemaType(str, Enum):
    baseline_metrics = "baseline_metrics"
    baseline_funnel_steps = "baseline_funnel_steps"
    cannibalization_matrix = "cannibalization_matrix"


class UploadDatasetSchemaType(str, Enum):
    baseline_metrics = "baseline_metrics"
    baseline_funnel_steps = "baseline_funnel_steps"
    cannibalization_matrix = "cannibalization_matrix"


class DatasetRecord(BaseModel):
    id: str
    dataset_name: str
    version: str
    scope: str
    schema_type: DatasetSchemaType
    format: DatasetFormat
    file_path: str
    checksum_sha256: str
    row_count: int
    columns: list[str]
    source_columns: list[str] | None = None
    column_mapping: dict[str, str] | None = None
    schema_version: str
    uploaded_by: str
    created_at: datetime
    is_latest: bool = False


class DatasetListResponse(BaseModel):
    items: list[DatasetRecord]


class DatasetPreviewResponse(BaseModel):
    dataset_name: str
    version: str
    limit: int = Field(ge=1, le=200)
    rows: list[dict[str, Any]]


class UploadDatasetResponse(BaseModel):
    dataset: DatasetRecord
