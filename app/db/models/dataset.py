from sqlalchemy import JSON, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.db.models.common import TimestampMixin, UuidPrimaryKeyMixin


class Dataset(Base, UuidPrimaryKeyMixin, TimestampMixin):
    __tablename__ = "datasets"
    __table_args__ = (
        UniqueConstraint("dataset_name", "version", "scope", name="uq_dataset_name_version_scope"),
        Index("ix_datasets_name_created", "dataset_name", "created_at"),
        Index("ix_datasets_schema_created", "schema_type", "created_at"),
        Index("ix_datasets_scope_schema_created", "scope", "schema_type", "created_at"),
    )

    dataset_name: Mapped[str] = mapped_column(String(255), nullable=False)
    version: Mapped[str] = mapped_column(String(32), nullable=False)
    scope: Mapped[str] = mapped_column(String(64), nullable=False, default="prod")
    schema_type: Mapped[str] = mapped_column(String(64), nullable=False)
    format: Mapped[str] = mapped_column(String(16), nullable=False)
    checksum_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    row_count: Mapped[int] = mapped_column(Integer, nullable=False)
    columns_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    schema_version: Mapped[str] = mapped_column(String(32), nullable=False, default="v1")
    uploaded_by: Mapped[str] = mapped_column(String(255), nullable=False)
