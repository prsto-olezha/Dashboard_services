from fastapi import APIRouter
from .v1.routes import dyeing_router_v1

dyeing_router = APIRouter(
	prefix="/dyeing", tags=['Dyeing']
)

dyeing_router.include_router(dyeing_router_v1)


