from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routers import config, dashboard, datasets, health, initiatives, learning, score, teams
from app.core.errors import register_exception_handlers
from app.core.logging import configure_logging, get_logger
from app.core.request_id import RequestIdMiddleware
from app.core.settings import get_settings

settings = get_settings()
configure_logging()
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI):
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    logger.info("app.startup", app_name=settings.app_name, data_dir=str(settings.data_dir))
    yield
    logger.info("app.shutdown")


app = FastAPI(
    title="Backlog Scoring Service",
    version="0.1.0",
    description="Expected value + uncertainty scoring service with auditability",
    lifespan=lifespan,
)
app.add_middleware(RequestIdMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    # Browsers reject wildcard origin when credentials are enabled.
    allow_credentials=settings.cors_origins != ["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

register_exception_handlers(app)

app.include_router(health.router)
app.include_router(datasets.router)
app.include_router(config.router)
app.include_router(score.router)
app.include_router(teams.router)
app.include_router(initiatives.router)
app.include_router(learning.router)
app.include_router(dashboard.router)
