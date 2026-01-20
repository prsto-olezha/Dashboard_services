from fastapi import FastAPI
import uvicorn
from core.logger import logger
from contextlib import asynccontextmanager
import asyncio
import locale
from scheduler.aps import init_scheduler
from core.db import init_mongo_db
from apps.e_com.v1.marketplaces.models import MarketplacesEcom
from apps.e_com_deviation.v1.models import EcomDeviation_model
from apps.weaving.v1.models import Weaving_model
from apps.tkachestvo.v1.models import Tkachestvo_model as Tkachestvo_model_v1
from apps.tkachestvo.v2.models import Tkachestvo_model as Tkachestvo_model_v2
from apps.tkachestvo.v3.models import Line_model as Tkachestvo_model_v3
from apps.yarn.v1.models import Yarn_model
from apps.dyeing.v1.models import Dyeing_model
from apps.sales_plan.v1.models import Sales_plan_model
from apps.sales.v1.models import Sales_model
from apps.finishing.v1.models import Finishing_model
from apps.sewing.v1.models import Sewing_model
from apps.warp.v1.models import Warp_model

#===========================================

from apps.yarn.v1.models import Yarn_model_backup

#===========================================
from apps.menu.routes import menu_router
from apps.e_com.routes import ecom_router
from apps.e_com_deviation.routes import ecom_deviation_router
from apps.weaving.routes import weaving_router
from apps.tkachestvo.routes import tkachestvo_router
from apps.yarn.routes import yarn_router
from apps.dyeing.routes import dyeing_router
from apps.sales_plan.routes import sales_plan_router
from apps.sales.routes import sales_router
from apps.finishing.routes import finishing_router
from apps.sewing.routes import sewing_router
from apps.warp.routes import warp_router

#===========================================
from fastapi.middleware.cors import CORSMiddleware

#==========
locale.setlocale(locale.LC_ALL,'ru_RU.UTF-8')

mongo_models_services = [
    MarketplacesEcom,
    EcomDeviation_model,
    Weaving_model,
    Tkachestvo_model_v1,
    Tkachestvo_model_v2,
    Tkachestvo_model_v3,
    Yarn_model,
    Dyeing_model,
    Sales_plan_model,
    Sales_model,
    Finishing_model,
    Sewing_model,
    Warp_model,
]

mongo_models_backup = [
    Yarn_model_backup,
]

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_mongo_db(models_services=mongo_models_services, models_backup=mongo_models_backup)
    init_scheduler()
    logger.info("Все запущено!")
    yield


async def on_startup():
    await asyncio.gather(
        asyncio.create_task(run_uvicorn()),
        )

async def run_uvicorn():
    config = uvicorn.Config(app, host="0.0.0.0", port=8000, log_level="info") #used port 8000
    server = uvicorn.Server(config)
    await server.serve()

app = FastAPI(lifespan=lifespan)

# Подключение маршрутов
app.include_router(router=menu_router)
app.include_router(router=ecom_router)
app.include_router(router=ecom_deviation_router)
app.include_router(router=weaving_router)
app.include_router(router=tkachestvo_router)
app.include_router(router=yarn_router)
app.include_router(router=dyeing_router)
app.include_router(router=sales_plan_router)
app.include_router(router=sales_router)
app.include_router(router=finishing_router)
app.include_router(router=sewing_router)
app.include_router(router=warp_router)


app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if __name__ == "__main__":
    asyncio.run(on_startup())
