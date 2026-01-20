from fastapi import APIRouter, Query, UploadFile, HTTPException
from .schemas.responses import EcomDeviationGetStatsResponse
from .utils import ecom_deviation_stats_aggregation, manual_write_data
from core.logger import logger
import datetime as dt
import io
from core.backend import e_com_deviation_data
#=========================

ecom_deviation_router_v1 = APIRouter(
    prefix="/ecom_deviation", tags=['E-com-deviation']
)


@ecom_deviation_router_v1.get('/get-stats', response_model=EcomDeviationGetStatsResponse)
async def ecom_deviation_get_stats(date_from: dt.datetime, date_to: dt.datetime,
                        offset: int = 0, limit: int = Query(default=100, le=100),
                        year: int = None, month: int = None):
        data = await ecom_deviation_stats_aggregation(date_from=date_from, date_to=date_to, skip=offset, limit=limit, year=year, month=month)
        return data


@ecom_deviation_router_v1.post('/upload-data')
async def ecom_deviation_upload_data(e_com: UploadFile, margin: UploadFile, date: dt.date = None, date_from: dt.datetime = None, date_to: dt.datetime = None):
    # try:
        # Считываем содержимое файла в байтовый буфер
        content1 = io.BytesIO(await e_com.read())
        content2 = io.BytesIO(await margin.read())
        if date_from != None and date_to != None:
            delta = dt.timedelta(days=1)
            current_date = date_from
            while current_date <= date_to:
                content1.seek(0)
                content2.seek(0)
                data = await e_com_deviation_data(current_date.date(), [content1, content2])
                data_obj = await manual_write_data(current_date.date(), data)
                current_date += delta
        elif date != None:
            # Обрабатываем данные
            data = await e_com_deviation_data(date, [content1, content2])
            data_obj = await manual_write_data(date, data)
            return {"status": "success", "message": "Data uploaded successfully", "data": data_obj}
        return {"status": "success", "message": "Data uploaded successfully"}
    # except Exception as ex:
    #     logger.error(f"Error processing file upload: {ex}")
    #     raise HTTPException(status_code=400, detail="Something went wrong!")

