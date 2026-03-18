from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime, Float, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.db.models.common import UuidPrimaryKeyMixin


class ABExperimentResult(Base, UuidPrimaryKeyMixin):
    __tablename__ = "ab_experiment_results"
    __table_args__ = (
        Index("ix_ab_results_screen_metric_end", "screen", "metric_driver", "end_at"),
        Index("ix_ab_results_segment_created", "segment_id", "created_at"),
        Index("ix_ab_results_initiative_created", "initiative_id", "created_at"),
        Index("ix_ab_results_quality_created", "quality_score", "created_at"),
        Index("ix_ab_results_scope_screen_metric_end", "scope", "screen", "metric_driver", "end_at"),
    )

    experiment_id: Mapped[str] = mapped_column(String(255), nullable=False)
    initiative_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    scope: Mapped[str] = mapped_column(String(64), nullable=False, default="prod")
    screen: Mapped[str] = mapped_column(String(128), nullable=False)
    segment_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    metric_driver: Mapped[str] = mapped_column(String(128), nullable=False)

    observed_uplift: Mapped[float] = mapped_column(Float, nullable=False)
    ci_low: Mapped[float | None] = mapped_column(Float, nullable=True)
    ci_high: Mapped[float | None] = mapped_column(Float, nullable=True)
    sample_size: Mapped[int] = mapped_column(Integer, nullable=False)
    significance_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    quality_score: Mapped[float] = mapped_column(Float, nullable=False)

    source: Mapped[str] = mapped_column(String(255), nullable=False)
    created_by: Mapped[str] = mapped_column(String(255), nullable=False)

    start_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    end_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
