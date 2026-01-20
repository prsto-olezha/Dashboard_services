from fastapi import APIRouter
from .v1.routes import sales_plan_router_v1

sales_plan_router = APIRouter(
	prefix="/sales_plan", tags=['Sales_plan']
)

sales_plan_router.include_router(sales_plan_router_v1)


