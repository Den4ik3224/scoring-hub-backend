from sqlalchemy import CheckConstraint, Date, Float, ForeignKey, Index, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.db.models.common import UuidPrimaryKeyMixin

import datetime


class BaselineMetricRow(Base, UuidPrimaryKeyMixin):
    __tablename__ = "baseline_metric_rows"
    __table_args__ = (
        UniqueConstraint("dataset_id", "segment_id", "date_start", "date_end", name="uq_baseline_metric_natural_key"),
        Index("ix_baseline_metric_rows_dataset_id", "dataset_id"),
        CheckConstraint("active_users >= 0", name="ck_bm_active_users_nonneg"),
        CheckConstraint("ordering_users >= 0", name="ck_bm_ordering_users_nonneg"),
        CheckConstraint("orders >= 0", name="ck_bm_orders_nonneg"),
        CheckConstraint("items >= 0", name="ck_bm_items_nonneg"),
        CheckConstraint("rto >= 0", name="ck_bm_rto_nonneg"),
        CheckConstraint("fm >= 0", name="ck_bm_fm_nonneg"),
    )

    dataset_id: Mapped[str] = mapped_column(String(36), ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False)
    segment_id: Mapped[str] = mapped_column(String(255), nullable=False)
    date_start: Mapped[datetime.date] = mapped_column(Date, nullable=False)
    date_end: Mapped[datetime.date] = mapped_column(Date, nullable=False)
    active_users: Mapped[float] = mapped_column(Float, nullable=False)
    ordering_users: Mapped[float] = mapped_column(Float, nullable=False)
    orders: Mapped[float] = mapped_column(Float, nullable=False)
    items: Mapped[float] = mapped_column(Float, nullable=False)
    rto: Mapped[float] = mapped_column(Float, nullable=False)
    fm: Mapped[float] = mapped_column(Float, nullable=False)
