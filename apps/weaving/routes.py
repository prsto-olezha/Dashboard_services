from fastapi import APIRouter
from .v1.routes import weaving_router_v1

weaving_router = APIRouter(
	prefix="/weaving", tags=['Weaving']
)

weaving_router.include_router(weaving_router_v1)


