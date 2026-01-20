from fastapi import APIRouter
from .v1.routes import sewing_router_v1

sewing_router = APIRouter(
	prefix="/sewing", tags=['Sewing']
)

sewing_router.include_router(sewing_router_v1)


