from fastapi import APIRouter, Query, UploadFile, HTTPException
from .schemas.responses import SalesPlanGetStatsResponse
from .utils import sales_plan_stats_aggregation, manual_write_data
from core.logger import logger
from core.backend import sales_plan_data
import datetime as dt
import io
#===================================

sales_plan_router_v1 = APIRouter(
    prefix="/v1", tags=['Sales_plan']
)


@sales_plan_router_v1.get('/get-stats', response_model=SalesPlanGetStatsResponse)
async def sales_plan_get_stats(date_from: dt.datetime, date_to: dt.datetime,
                                 offset: int = 0, limit: int = Query(default=100, le=100)):
        data = await sales_plan_stats_aggregation(date_from=date_from, date_to=date_to, skip=offset, limit=limit)
        return data


@sales_plan_router_v1.post('/upload-data')
async def sales_plan_upload_data(file: UploadFile):
    try:
        # Считываем содержимое файла в байтовый буфер
        content = io.BytesIO(await file.read())
        
        # Обрабатываем данные
        data = await sales_plan_data(content)
        sales_plan_obj = await manual_write_data(data)
        
        return {"status": "success", "message": "Data uploaded successfully", "data": sales_plan_obj}
    except Exception as ex:
        logger.error(f"Error processing file upload: {ex}")
        raise HTTPException(status_code=400, detail="Something went wrong!")

