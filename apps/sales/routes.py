from fastapi import APIRouter
from .v1.routes import sales_router_v1

sales_router = APIRouter(
	prefix="/sales", tags=['Sales']
)

sales_router.include_router(sales_router_v1)


