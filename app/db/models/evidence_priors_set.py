from sqlalchemy import Boolean, Index, String, UniqueConstraint
from sqlalchemy import JSON
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.db.models.common import TimestampMixin, UuidPrimaryKeyMixin


class EvidencePriorsSet(Base, UuidPrimaryKeyMixin, TimestampMixin):
    __tablename__ = "evidence_priors_sets"
    __table_args__ = (
        UniqueConstraint("name", "version", name="uq_evidence_priors_name_version"),
        Index("ix_evidence_priors_name_created", "name", "created_at"),
    )

    name: Mapped[str] = mapped_column(String(255), nullable=False)
    version: Mapped[str] = mapped_column(String(32), nullable=False)
    priors_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    is_default: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_by: Mapped[str] = mapped_column(String(255), nullable=False)
