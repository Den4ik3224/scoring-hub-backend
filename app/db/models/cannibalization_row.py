from sqlalchemy import CheckConstraint, Float, ForeignKey, Index, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.db.models.common import UuidPrimaryKeyMixin


class CannibalizationRow(Base, UuidPrimaryKeyMixin):
    __tablename__ = "cannibalization_rows"
    __table_args__ = (
        UniqueConstraint(
            "dataset_id", "from_screen", "to_screen", "segment_id",
            name="uq_cannibalization_natural_key",
        ),
        Index("ix_cannibalization_rows_dataset_id", "dataset_id"),
        CheckConstraint("cannibalization_rate >= 0 AND cannibalization_rate <= 1", name="ck_cr_rate_range"),
    )

    dataset_id: Mapped[str] = mapped_column(String(36), ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False)
    from_screen: Mapped[str] = mapped_column(String(255), nullable=False)
    to_screen: Mapped[str] = mapped_column(String(255), nullable=False)
    segment_id: Mapped[str] = mapped_column(String(255), nullable=False)
    cannibalization_rate: Mapped[float] = mapped_column(Float, nullable=False)
