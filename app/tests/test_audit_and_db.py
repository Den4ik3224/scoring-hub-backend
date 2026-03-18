import pyarrow as pa
import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.api.schemas.score import ScoreRunRequest
from app.core.settings import Settings
from app.db.base import Base
from app.db.models import Dataset
from app.services.audit import build_assumptions_snapshot_hash, persist_scoring_run
from app.services.scoring_engine import run_scoring

from .helpers_monthly import monthly_baseline_table, resolved_inputs_stub


@pytest.mark.asyncio
async def test_duplicate_dataset_name_version_conflict() -> None:
    engine = create_async_engine(
        "sqlite+aiosqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with session_maker() as session:
        first = Dataset(
            dataset_name="baseline",
            version="v1",
            schema_type="baseline_metrics",
            format="csv",
            file_path="/data/baseline/v1/data.csv",
            checksum_sha256="x" * 64,
            row_count=1,
            columns_json={"columns": []},
            schema_version="v1",
            uploaded_by="tester",
        )
        second = Dataset(
            dataset_name="baseline",
            version="v1",
            schema_type="baseline_metrics",
            format="csv",
            file_path="/data/baseline/v1/data2.csv",
            checksum_sha256="y" * 64,
            row_count=1,
            columns_json={"columns": []},
            schema_version="v1",
            uploaded_by="tester",
        )
        session.add(first)
        await session.commit()

        session.add(second)
        with pytest.raises(IntegrityError):
            await session.commit()

    await engine.dispose()


@pytest.mark.asyncio
async def test_persist_run_and_snapshot_hash() -> None:
    engine = create_async_engine(
        "sqlite+aiosqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_maker = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    baseline = monthly_baseline_table(
        active_users=1000.0,
        ordering_users=100.0,
        orders=200.0,
        items=200.0,
        rto=4000.0,
        fm=1200.0,
    )

    dataset = Dataset(
        dataset_name="baseline",
        version="v1",
        schema_type="baseline_metrics",
        format="csv",
        file_path="/data/baseline/v1/data.csv",
        checksum_sha256="x" * 64,
        row_count=1,
        columns_json={"columns": []},
        schema_version="v1",
        uploaded_by="tester",
    )

    resolved_inputs = resolved_inputs_stub(baseline=baseline)
    resolved_inputs.baseline_dataset = dataset
    resolved_inputs.resolved_inputs_json["datasets"]["baseline_metrics"]["checksum"] = "x" * 64

    payload = ScoreRunRequest(
        initiative_name="Audit",
        segments=[{"id": "s1", "penetration": 0.5, "uplifts": {"conversion": 0.1}}],
        screens=["home"],
        metric_targets=[],
        p_success=0.5,
        confidence=0.8,
        effort_cost=100,
        strategic_weight=1.0,
        learning_value=1.0,
        horizon_weeks=4,
        monte_carlo={"enabled": True, "n": 1000, "seed": 11},
    )

    result = run_scoring(payload, resolved_inputs, mc_max_n=50_000)
    expected_hash = build_assumptions_snapshot_hash(payload, resolved_inputs, "unit-test")

    async with session_maker() as session:
        persisted = await persist_scoring_run(
            session,
            payload=payload,
            resolved_inputs=resolved_inputs,
            scoring_result=result,
            settings=Settings(
                database_url="sqlite+aiosqlite://",
                data_dir="/tmp",
                git_sha="unit-test",
            ),
            created_by="tester",
            initiative_db_id=None,
        )

        assert persisted.assumptions_snapshot_hash == expected_hash
        assert persisted.request_payload_json["initiative_name"] == "Audit"
        assert persisted.resolved_inputs_json["datasets"]["baseline_metrics"]["version"] == "v1"
        assert persisted.rng_seed == 11

    await engine.dispose()
