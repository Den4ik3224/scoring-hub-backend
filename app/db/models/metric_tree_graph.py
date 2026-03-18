from sqlalchemy import Boolean, Index, String, UniqueConstraint
from sqlalchemy import JSON
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.db.models.common import TimestampMixin, UuidPrimaryKeyMixin


class MetricTreeGraph(Base, UuidPrimaryKeyMixin, TimestampMixin):
    __tablename__ = "metric_tree_graphs"
    __table_args__ = (
        UniqueConstraint("template_name", "version", name="uq_metric_tree_graph_name_version"),
        Index("ix_metric_tree_graph_name_created", "template_name", "created_at"),
    )

    template_name: Mapped[str] = mapped_column(String(255), nullable=False)
    version: Mapped[str] = mapped_column(String(32), nullable=False)
    graph_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    is_default: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_by: Mapped[str] = mapped_column(String(255), nullable=False)
