from fastapi import APIRouter, Query, UploadFile, HTTPException
from .schemas.responses import TkachestvoGetStatsResponse
from .utils import manual_write_data
from core.logger import logger
# from core.backend import tkachestvo_data
from .backend import tkachestvo_data
import datetime as dt
import traceback
import io
#===================================

tkachestvo_router_v3 = APIRouter(
    prefix="/v3", tags=['Tkachestvo']
)


# @tkachestvo_router_v3.get('/get-stats', response_model=TkachestvoGetStatsResponse)
# async def tkachestvo_get_stats(date_from: dt.datetime, date_to: dt.datetime,
#                         offset: int = 0, limit: int = Query(default=100, le=100), summary: bool = False,
#                         year: int = None, month: int = None):
#     if summary is False:
#         data = await tkachestvo_stats_aggregation(date_from=date_from, date_to=date_to, skip=offset, limit=limit, year = year, month = month)
#         return data
#     if summary is True:
#         data = await sum_tkachestvo_stats_aggregation(date_from=date_from, date_to=date_to, year = year, month = month)
#         return data


@tkachestvo_router_v3.post('/upload-data')
async def tkachestvo_upload_data(Weaving: UploadFile, date: dt.date = None, date_from: dt.datetime = None, date_to: dt.datetime = None):
    try:
        content = io.BytesIO(await Weaving.read())
        if date_from != None and date_to != None:
            delta = dt.timedelta(days=1)
            current_date = date_from
            while current_date <= date_to:
                content.seek(0)
                data = await tkachestvo_data(current_date.date(), content)
                data_obj = await manual_write_data(current_date.date(), data)
                current_date += delta
        elif date != None:
            # Обрабатываем данные
            data = await tkachestvo_data(date, content)
            data_obj = await manual_write_data(date, data)
            return {"status": "success", "message": "Data uploaded successfully", "data": data_obj}
        return {"status": "success", "message": "Data uploaded successfully"}
    except Exception as ex:
        print(traceback.format_exc())
        logger.error(f"Error processing file upload: {ex}")
        raise HTTPException(status_code=400, detail="Something went wrong!")