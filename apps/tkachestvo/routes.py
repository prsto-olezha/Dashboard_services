from fastapi import APIRouter
from .v1.routes import tkachestvo_router_v1
from .v2.routes import tkachestvo_router_v2
from .v3.routes import tkachestvo_router_v3

tkachestvo_router = APIRouter(
	prefix="/tkachestvo", tags=['Tkachestvo']
)

tkachestvo_router.include_router(router=tkachestvo_router_v1)
tkachestvo_router.include_router(router=tkachestvo_router_v2)
tkachestvo_router.include_router(router=tkachestvo_router_v3)