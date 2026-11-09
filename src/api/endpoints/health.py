from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, status

from core.utils import check_startup_dependencies
from schemas.health import HealthState, HealthStatus

router = APIRouter(prefix="/health", tags=["Health"])


@router.get(
    "/live",
    response_model=HealthStatus,
    summary="Liveness check",
)
async def liveness_check() -> HealthStatus:
    return HealthStatus(
        status=HealthState.OK,
        time=datetime.now(UTC),
    )


@router.get(
    "/ready",
    response_model=HealthStatus,
    summary="Readiness check",
)
async def readiness_check(request: Request) -> HealthStatus:
    if not getattr(request.app.state, "startup_ok", False):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=HealthState.NOT_OK,
        )
    return HealthStatus(
        status=HealthState.OK,
        time=datetime.now(UTC),
    )


@router.get(
    "/startup",
    response_model=HealthStatus,
    summary="Startup dependency check",
)
async def startup_check(
    startup_dependencies_ok: Annotated[
        bool,
        Depends(check_startup_dependencies),
    ],
) -> HealthStatus:
    if not startup_dependencies_ok:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service dependencies not ready",
        )
    return HealthStatus(
        status=HealthState.OK,
        time=datetime.now(UTC),
    )
