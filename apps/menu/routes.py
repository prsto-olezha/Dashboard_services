from fastapi import APIRouter
from .v1.routes import menu_router_v1

menu_router = APIRouter(
	prefix="/menu", tags=['menu']
)

menu_router.include_router(menu_router_v1)


