from sqlalchemy import CheckConstraint, Date, Float, ForeignKey, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.db.models.common import UuidPrimaryKeyMixin

import datetime


class FunnelStepRow(Base, UuidPrimaryKeyMixin):
    __tablename__ = "funnel_step_rows"
    __table_args__ = (
        UniqueConstraint(
            "dataset_id", "segment_id", "screen", "step_id", "date_start", "date_end",
            name="uq_funnel_step_natural_key",
        ),
        Index("ix_funnel_step_rows_dataset_id", "dataset_id"),
        CheckConstraint("step_order >= 1", name="ck_fs_step_order_min"),
        CheckConstraint("entered_users >= 0", name="ck_fs_entered_users_nonneg"),
        CheckConstraint("advanced_users >= 0", name="ck_fs_advanced_users_nonneg"),
    )

    dataset_id: Mapped[str] = mapped_column(String(36), ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False)
    segment_id: Mapped[str] = mapped_column(String(255), nullable=False)
    screen: Mapped[str] = mapped_column(String(255), nullable=False)
    step_id: Mapped[str] = mapped_column(String(255), nullable=False)
    step_name: Mapped[str] = mapped_column(String(255), nullable=False)
    step_order: Mapped[int] = mapped_column(Integer, nullable=False)
    date_start: Mapped[datetime.date] = mapped_column(Date, nullable=False)
    date_end: Mapped[datetime.date] = mapped_column(Date, nullable=False)
    entered_users: Mapped[float] = mapped_column(Float, nullable=False)
    advanced_users: Mapped[float] = mapped_column(Float, nullable=False)
