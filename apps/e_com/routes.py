from fastapi import APIRouter
from .v1.routes import ecom_router_v1

ecom_router = APIRouter(
	prefix="/e-com", tags=['E-com']
)

ecom_router.include_router(ecom_router_v1)


