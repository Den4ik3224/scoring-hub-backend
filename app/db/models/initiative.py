from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Index, String
from sqlalchemy import JSON
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.db.models.common import TimestampMixin, UpdatedAtMixin, UuidPrimaryKeyMixin


class Initiative(Base, UuidPrimaryKeyMixin, TimestampMixin, UpdatedAtMixin):
    __tablename__ = "initiatives"
    __table_args__ = (
        Index("ix_initiatives_external_key", "external_key"),
        Index("ix_initiatives_owner_status_updated", "owner_team_id", "status", "updated_at"),
    )

    external_key: Mapped[str | None] = mapped_column(String(255), nullable=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(String(4096), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="draft")
    owner_team_id: Mapped[str] = mapped_column(String(36), ForeignKey("teams.id"), nullable=False)
    created_by_user_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_by_email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    tags_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    archived_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
