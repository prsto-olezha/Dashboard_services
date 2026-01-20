from fastapi import APIRouter
from .marketplaces.routes import marketplaces_router

ecom_router_v1 = APIRouter(
	prefix="/v1", tags=['E-com']
)

ecom_router_v1.include_router(marketplaces_router)
