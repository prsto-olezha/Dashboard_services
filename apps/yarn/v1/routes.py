from fastapi import APIRouter, Query, UploadFile, HTTPException
from .schemas.responses import YarnGetStatsResponse
from .utils import yarn_stats_aggregation, sum_yarn_stats_aggregation, manual_write_data, yarn_screen_stats_aggregation
from core.logger import logger
from core.backend import yarn_data
import datetime as dt
import io
#===================================

yarn_router_v1 = APIRouter(
    prefix="/v1", tags=['Yarn']
)


@yarn_router_v1.get('/get-stats', response_model=YarnGetStatsResponse)
async def yarn_get_stats(date_from: dt.datetime, date_to: dt.datetime,
                        offset: int = 0, limit: int = Query(default=100, le=100),
                        year: int = None, month: int = None,
                        summary: bool = False,):
#                         state_on_date: bool = Query(False),
# ):
#     # Проверка, что screen_date обязателен, если state_on_date=True
#     if state_on_date:
#         if state_on_date and screen_date is None:
#             raise HTTPException(status_code=400, detail="screen_date обязателен, если state_on_date=True")
#         else:
#             return await yarn_screen_stats_aggregation(date_from=date_from, date_to=date_to, skip=offset, limit=limit, year = year, month = month)
    
    if summary is False:
        data = await yarn_stats_aggregation(date_from=date_from, date_to=date_to, skip=offset, limit=limit, year = year, month = month)
        return data
    
    if summary is True:
        data = await sum_yarn_stats_aggregation(date_from=date_from, date_to=date_to, year = year, month = month)
        return data
    
    

@yarn_router_v1.post('/upload-data')
async def yarn_upload_data(file: UploadFile, date: dt.date = None, date_from: dt.datetime = None, date_to: dt.datetime = None):
    # try:
        content = io.BytesIO(await file.read())
        if date_from != None and date_to != None:
            delta = dt.timedelta(days=1)
            current_date = date_from
            while current_date <= date_to:
                content.seek(0)
                data = await yarn_data(current_date.date(), content)
                data_obj = await manual_write_data(current_date.date(), data)
                current_date += delta
        elif date != None:
            # Обрабатываем данные
            data = await yarn_data(date, content)
            data_obj = await manual_write_data(date, data)
            return {"status": "success", "message": "Data uploaded successfully", "data": data_obj}
        return {"status": "success", "message": "Data uploaded successfully"}
    # except Exception as ex:
    #     logger.error(f"Error processing file upload: {ex}")
    #     raise HTTPException(status_code=400, detail="Something went wrong!")

