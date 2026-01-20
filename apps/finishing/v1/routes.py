from fastapi import APIRouter, Query, UploadFile, HTTPException
from .schemas.responses import FinishingGetStatsResponse
from .utils import finishing_stats_aggregation, sum_finishing_stats_aggregation, manual_write_data
from core.logger import logger
from core.backend import finishing_data
import datetime as dt
import io
#===================================

finishing_router_v1 = APIRouter(
    prefix="/v1", tags=['Finishing']
)


@finishing_router_v1.get('/get-stats', response_model=FinishingGetStatsResponse)
async def finishing_get_stats(date_from: dt.datetime, date_to: dt.datetime,
                                 offset: int = 0, limit: int = Query(default=100, le=100), summary: bool = False):
    if summary is False:
        data = await finishing_stats_aggregation(date_from=date_from, date_to=date_to, skip=offset, limit=limit)
        return data
    if summary is True:
        data = await sum_finishing_stats_aggregation(date_from=date_from, date_to=date_to)
        return data


@finishing_router_v1.post('/upload-data')
async def finishing_upload_data(file: UploadFile):
    try:
        # Считываем содержимое файла в байтовый буфер
        content = io.BytesIO(await file.read())
        
        # Обрабатываем данные
        data = await finishing_data(content)
        finishing_obj = await manual_write_data(data)
        
        return {"status": "success", "message": "Data uploaded successfully", "data": finishing_obj}
    except Exception as ex:
        logger.error(f"Error processing file upload: {ex}")
        raise HTTPException(status_code=400, detail="Something went wrong!")

