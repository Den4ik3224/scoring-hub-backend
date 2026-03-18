from sqlalchemy import Float, ForeignKey, Index, Integer, String
from sqlalchemy import JSON
from sqlalchemy import event
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.db.models.common import TimestampMixin, UuidPrimaryKeyMixin


class InitiativeVersion(Base, UuidPrimaryKeyMixin, TimestampMixin):
    __tablename__ = "initiative_versions"
    __table_args__ = (
        Index("ix_initiative_versions_initiative_version", "initiative_id", "version_number", unique=True),
        Index("ix_initiative_versions_initiative_created", "initiative_id", "created_at"),
    )

    initiative_id: Mapped[str] = mapped_column(String(36), ForeignKey("initiatives.id"), nullable=False)
    version_number: Mapped[int] = mapped_column(Integer, nullable=False)

    title_override: Mapped[str | None] = mapped_column(String(255), nullable=True)
    description_override: Mapped[str | None] = mapped_column(String(4096), nullable=True)
    data_scope: Mapped[str] = mapped_column(String(64), nullable=False, default="prod")

    screens_json: Mapped[list] = mapped_column(JSON, nullable=False)
    segments_json: Mapped[list] = mapped_column(JSON, nullable=False)
    metric_targets_json: Mapped[list] = mapped_column(JSON, nullable=False)
    assumptions_json: Mapped[dict] = mapped_column(JSON, nullable=False)

    p_success: Mapped[float | None] = mapped_column(Float, nullable=True)
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    evidence_type: Mapped[str | None] = mapped_column(String(255), nullable=True)
    effort_cost: Mapped[float | None] = mapped_column(Float, nullable=True)
    strategic_weight: Mapped[float | None] = mapped_column(Float, nullable=True)
    learning_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    horizon_weeks: Mapped[int | None] = mapped_column(Integer, nullable=True)
    decay_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    discount_rate_annual: Mapped[float | None] = mapped_column(Float, nullable=True)
    cannibalization_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    interactions_json: Mapped[list | None] = mapped_column(JSON, nullable=True)

    created_by_user_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_by_email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    change_comment: Mapped[str | None] = mapped_column(String(4096), nullable=True)


@event.listens_for(InitiativeVersion, "before_update", propagate=True)
def _prevent_initiative_version_update(*_: object, **__: object) -> None:
    raise ValueError("InitiativeVersion is immutable and cannot be updated")
