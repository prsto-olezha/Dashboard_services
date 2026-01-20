from fastapi import APIRouter
from .v1.routes import finishing_router_v1

finishing_router = APIRouter(
	prefix="/finishing", tags=['Finishing']
)

finishing_router.include_router(finishing_router_v1)


