from fastapi import APIRouter
from .v1.routes import yarn_router_v1

yarn_router = APIRouter(
	prefix="/yarn", tags=['Yarn']
)

yarn_router.include_router(yarn_router_v1)


