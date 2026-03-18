import hashlib
from pathlib import Path

from fastapi import APIRouter, Depends, File, Query, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.schemas.dataset import (
    DatasetFormat,
    DatasetListResponse,
    DatasetPreviewResponse,
    DatasetRecord,
    DatasetSchemaType,
    UploadDatasetSchemaType,
    UploadDatasetResponse,
)
from app.core.errors import NotFoundError, ValidationError
from app.core.security import Principal, get_current_principal, require_admin
from app.core.settings import Settings, get_settings
from app.db.models import Dataset
from app.db.repositories import datasets as dataset_repo
from app.db.session import get_session
from app.services.dataset_registry import (
    fetch_dataset_or_404,
    generate_dataset_version,
    store_and_register_dataset,
    table_preview_from_bytes,
)

router = APIRouter(prefix="/datasets", tags=["datasets"])


def _to_record(model: Dataset, is_latest: bool = False) -> DatasetRecord:
    columns_json = model.columns_json or {}
    return DatasetRecord(
        id=model.id,
        dataset_name=model.dataset_name,
        version=model.version,
        scope=model.scope,
        schema_type=model.schema_type,
        format=model.format,
        file_path=model.file_path,
        checksum_sha256=model.checksum_sha256,
        row_count=model.row_count,
        columns=columns_json.get("columns", []),
        source_columns=columns_json.get("source_columns"),
        column_mapping=columns_json.get("column_mapping"),
        schema_version=model.schema_version,
        uploaded_by=model.uploaded_by,
        created_at=model.created_at,
        is_latest=is_latest,
    )


@router.post("/upload", response_model=UploadDatasetResponse)
async def upload_dataset(
    dataset_name: str = Query(min_length=1, max_length=255),
    dataset_version: str | None = Query(default=None, min_length=1, max_length=32),
    format: DatasetFormat = Query(alias="format"),
    schema_type: UploadDatasetSchemaType = Query(alias="schema_type"),
    scope: str = Query(default="prod", min_length=1, max_length=64),
    column_mapping: str | None = Query(
        default=None,
        description="Optional JSON object in canonical->source format, e.g. {'segment_id':'Segment'}",
    ),
    file: UploadFile = File(...),
    principal: Principal = Depends(require_admin),
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> UploadDatasetResponse:
    if file.filename is None:
        raise ValidationError("Uploaded file must have a filename")

    version = dataset_version or generate_dataset_version()
    parsed_mapping = None
    if column_mapping:
        import json

        try:
            parsed_mapping = json.loads(column_mapping)
        except json.JSONDecodeError as exc:
            raise ValidationError("`column_mapping` must be a valid JSON object string") from exc
        if not isinstance(parsed_mapping, dict):
            raise ValidationError("`column_mapping` must be a JSON object in canonical->source format")

    record = await store_and_register_dataset(
        session,
        upload_file=file,
        dataset_name=dataset_name,
        version=version,
        schema_type=schema_type.value,
        fmt=format.value,
        uploaded_by=principal.sub,
        max_upload_mb=settings.max_upload_mb,
        scope=scope,
        column_mapping=parsed_mapping,
    )
    return UploadDatasetResponse(dataset=_to_record(record))


@router.get("", response_model=DatasetListResponse)
async def list_registered_datasets(
    scope: str | None = Query(default=None, min_length=1, max_length=64),
    _: Principal = Depends(get_current_principal),
    session: AsyncSession = Depends(get_session),
) -> DatasetListResponse:
    records = await dataset_repo.list_datasets(session, scope=scope)

    latest_by_name: dict[tuple[str, str], str] = {}
    for record in records:
        latest_by_name.setdefault((record.dataset_name, record.scope), record.version)

    items = [
        _to_record(record, is_latest=latest_by_name.get((record.dataset_name, record.scope)) == record.version)
        for record in records
    ]
    return DatasetListResponse(items=items)


@router.get("/{dataset_name}/versions", response_model=DatasetListResponse)
async def list_dataset_versions(
    dataset_name: str,
    scope: str | None = Query(default=None, min_length=1, max_length=64),
    _: Principal = Depends(get_current_principal),
    session: AsyncSession = Depends(get_session),
) -> DatasetListResponse:
    records = await dataset_repo.list_versions_for_dataset(session, dataset_name=dataset_name, scope=scope)
    if not records:
        raise NotFoundError(f"Dataset `{dataset_name}` not found")

    latest_version = records[0].version
    items = [_to_record(record, is_latest=(record.version == latest_version)) for record in records]
    return DatasetListResponse(items=items)


@router.get("/{dataset_name}/{version}/preview", response_model=DatasetPreviewResponse)
async def preview_dataset(
    dataset_name: str,
    version: str,
    limit: int = Query(default=50, ge=1, le=200),
    scope: str | None = Query(default=None, min_length=1, max_length=64),
    _: Principal = Depends(get_current_principal),
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> DatasetPreviewResponse:
    if limit > settings.preview_max_limit:
        raise ValidationError(f"Preview limit cannot exceed {settings.preview_max_limit}")

    record = await fetch_dataset_or_404(session, dataset_name=dataset_name, version=version, scope=scope)
    blob = await dataset_repo.get_dataset_blob(session, record.id)
    if not blob:
        raise NotFoundError(f"Dataset content not found for `{dataset_name}` version `{version}`")
    rows = table_preview_from_bytes(data=blob, fmt=record.format, limit=limit)
    return DatasetPreviewResponse(dataset_name=dataset_name, version=version, limit=limit, rows=rows)


@router.post("/migrate-to-db")
async def migrate_datasets_to_db(
    principal: Principal = Depends(require_admin),
    session: AsyncSession = Depends(get_session),
) -> dict:
    """Temporary endpoint: migrate dataset files from filesystem to DB blobs."""
    records = await dataset_repo.list_datasets(session)
    migrated = 0
    skipped = 0
    errors: list[str] = []

    for record in records:
        existing_blob = await dataset_repo.get_dataset_blob(session, record.id)
        if existing_blob:
            skipped += 1
            continue

        if not record.file_path:
            errors.append(f"{record.dataset_name}:{record.version} — no file_path")
            continue

        file_path = Path(record.file_path)
        if not file_path.exists():
            errors.append(f"{record.dataset_name}:{record.version} — file not found: {record.file_path}")
            continue

        try:
            data = file_path.read_bytes()
            checksum = hashlib.sha256(data).hexdigest()
            if checksum != record.checksum_sha256:
                errors.append(
                    f"{record.dataset_name}:{record.version} — checksum mismatch "
                    f"(expected {record.checksum_sha256[:8]}..., got {checksum[:8]}...)"
                )
                continue
            await dataset_repo.store_dataset_blob(session, record.id, data)
            migrated += 1
        except Exception as exc:
            errors.append(f"{record.dataset_name}:{record.version} — {exc}")

    await session.commit()
    return {"migrated": migrated, "skipped": skipped, "errors": errors}
