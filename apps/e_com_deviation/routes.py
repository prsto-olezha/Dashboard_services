from fastapi import APIRouter
from .v1.routes import ecom_deviation_router_v1

ecom_deviation_router = APIRouter(
	prefix="/e-com-deviation", tags=['E-com-deviation']
)

ecom_deviation_router.include_router(ecom_deviation_router_v1)


