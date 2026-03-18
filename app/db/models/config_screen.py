from sqlalchemy import Boolean, Index, String
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.db.models.common import TimestampMixin, UpdatedAtMixin, UuidPrimaryKeyMixin


class ConfigScreen(Base, UuidPrimaryKeyMixin, TimestampMixin, UpdatedAtMixin):
    __tablename__ = "config_screens"
    __table_args__ = (
        Index("ix_config_screens_slug", "slug", unique=True),
        Index("ix_config_screens_active_updated", "is_active", "updated_at"),
    )

    slug: Mapped[str] = mapped_column(String(128), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(String(4096), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
