from sqlalchemy import ForeignKey, Index, Integer, String
from sqlalchemy import JSON
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.db.models.common import TimestampMixin, UuidPrimaryKeyMixin


class ScoringRun(Base, UuidPrimaryKeyMixin, TimestampMixin):
    __tablename__ = "scoring_runs"
    __table_args__ = (
        Index("ix_scoring_runs_created", "created_at"),
        Index("ix_scoring_runs_initiative", "initiative_id"),
        Index("ix_scoring_runs_initiative_version_created", "initiative_version_id", "created_at"),
        Index("ix_scoring_runs_triggered_by_created", "triggered_by_user_id", "created_at"),
        Index("ix_scoring_runs_run_purpose_created", "run_purpose", "created_at"),
        Index("ix_scoring_runs_status_created", "run_status", "created_at"),
        Index("ix_scoring_runs_snapshot_hash", "assumptions_snapshot_hash"),
    )

    initiative_id: Mapped[str | None] = mapped_column(String(36), ForeignKey("initiatives.id"), nullable=True)
    initiative_version_id: Mapped[str | None] = mapped_column(String(36), ForeignKey("initiative_versions.id"), nullable=True)
    initiative_name: Mapped[str] = mapped_column(String(255), nullable=False)

    request_payload_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    resolved_inputs_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    assumptions_snapshot_hash: Mapped[str] = mapped_column(String(64), nullable=False)

    rng_seed: Mapped[int] = mapped_column(Integer, nullable=False)
    monte_carlo_n: Mapped[int] = mapped_column(Integer, nullable=False)
    code_version: Mapped[str] = mapped_column(String(255), nullable=False)

    deterministic_output_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    probabilistic_output_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    segment_breakdown_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    node_contributions_json: Mapped[dict] = mapped_column(JSON, nullable=False)

    created_by: Mapped[str] = mapped_column(String(255), nullable=False)
    triggered_by_user_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    triggered_by_email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    triggered_by_role: Mapped[str | None] = mapped_column(String(64), nullable=True)
    run_label: Mapped[str | None] = mapped_column(String(255), nullable=True)
    run_purpose: Mapped[str | None] = mapped_column(String(32), nullable=True)
    run_status: Mapped[str] = mapped_column(String(16), nullable=False, default="success")
    error_message: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    recompute_of_run_id: Mapped[str | None] = mapped_column(String(36), ForeignKey("scoring_runs.id"), nullable=True)
