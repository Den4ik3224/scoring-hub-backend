from sqlalchemy import ForeignKey, LargeBinary, String
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.db.models.common import TimestampMixin, UuidPrimaryKeyMixin


class DatasetBlob(Base, UuidPrimaryKeyMixin, TimestampMixin):
    __tablename__ = "dataset_blobs"

    dataset_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("datasets.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    data: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
