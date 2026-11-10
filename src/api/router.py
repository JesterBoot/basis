from fastapi import APIRouter, Depends

from api.deps import require_api_key
from api.endpoints import health, payments

api_router = APIRouter()
api_router.include_router(health.router)

v1_router = APIRouter(prefix="/api/v1", dependencies=[Depends(require_api_key)])
v1_router.include_router(payments.router)

api_router.include_router(v1_router)
