from fastapi import APIRouter, Query, UploadFile, HTTPException
from core.logger import logger
from scheduler.aps import write_data
import datetime as dt
import io
import traceback
#=========================

menu_router_v1 = APIRouter(
    prefix="/menu", tags=['menu']
)


@menu_router_v1.get('/get-stats')
async def file_parse():
    try:
        await write_data()
    except Exception:
        print(traceback.format_exc())
