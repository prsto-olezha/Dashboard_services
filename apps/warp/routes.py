from fastapi import APIRouter
from .v1.routes import warp_router_v1

warp_router = APIRouter(
	prefix="/warp", tags=['Warp']
)

warp_router.include_router(warp_router_v1)


