from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.schemas.dashboard import DashboardSummaryResponse
from app.core.security import Principal, get_current_principal
from app.db.session import get_session
from app.services.dashboard_service import build_dashboard_summary

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get("/summary", response_model=DashboardSummaryResponse)
async def get_dashboard_summary(
    _: Principal = Depends(get_current_principal),
    session: AsyncSession = Depends(get_session),
) -> DashboardSummaryResponse:
    return await build_dashboard_summary(session)
