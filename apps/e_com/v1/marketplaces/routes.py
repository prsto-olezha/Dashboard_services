from fastapi import APIRouter, Query, UploadFile, HTTPException
from .schemas.responses import MarketplacesGetStatsResponse
from .utils import marketplaces_stats_aggregation, sum_marketplaces_stats_aggregation, manual_write_data
from core.logger import logger
import datetime as dt
import io
from core.backend import e_com_data
#=========================

marketplaces_router = APIRouter(
    prefix="/marketplaces", tags=['E-com']
)


@marketplaces_router.get('/get-stats', response_model=MarketplacesGetStatsResponse)
async def marketplaces_get_stats(date_from: dt.datetime, date_to: dt.datetime,
                        offset: int = 0, limit: int = Query(default=100, le=100), summary: bool = False,
                        year: int = None, month: int = None):
    if summary is False:
        data = await marketplaces_stats_aggregation(date_from=date_from, date_to=date_to, skip=offset, limit=limit, year=year, month=month)
        return data
    if summary is True:
        data = await sum_marketplaces_stats_aggregation(date_from=date_from, date_to=date_to, year=year, month=month)
        return data


@marketplaces_router.post('/upload-data')
async def marketplaces_upload_data(file: UploadFile, date: dt.date = None, date_from: dt.datetime = None, date_to: dt.datetime = None):
    # try:
        # Считываем содержимое файла в байтовый буфер
        content = io.BytesIO(await file.read())
        
        if date_from != None and date_to != None:
            delta = dt.timedelta(days=1)
            current_date = date_from
            while current_date <= date_to:
                content.seek(0)
                data = await e_com_data(current_date.date(), content)
                data_obj = await manual_write_data(current_date.date(), data)
                current_date += delta
        elif date != None:
            # Обрабатываем данные
            data = await e_com_data(date, content)
            data_obj = await manual_write_data(date, data)
            return {"status": "success", "message": "Data uploaded successfully", "data": data_obj}
        return {"status": "success", "message": "Data uploaded successfully"}
    # except Exception as ex:
    #     logger.error(f"Error processing file upload: {ex}")
    #     raise HTTPException(status_code=400, detail="Something went wrong!")

