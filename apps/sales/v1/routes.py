from fastapi import APIRouter, Query, UploadFile, HTTPException, File
from .schemas.responses import SalesGetStatsResponse
from .utils import sales_stats_aggregation, manual_write_data
from core.logger import logger
from core.backend import sales_data
import datetime as dt
import io
#===================================

sales_router_v1 = APIRouter(
    prefix="/v1", tags=['Sales']
)


@sales_router_v1.get('/get-stats', response_model=SalesGetStatsResponse)
async def sales_get_stats(date_from: dt.datetime | None = Query(default=None), date_to: dt.datetime | None = Query(default=None),
                                 offset: int = 0, limit: int = Query(default=100, le=100)):
    if date_from is None and date_to is None:
        date_from = dt.datetime.now(dt.timezone.utc).replace(day=1).date()
        date_to = dt.datetime.now(dt.timezone.utc).replace(month=dt.datetime.now(dt.timezone.utc).month+1, day=1).date()
    data = await sales_stats_aggregation(date_from=date_from, date_to=date_to, skip=offset, limit=limit)
    return data


@sales_router_v1.post('/upload-data')
async def sales_upload_data(date: dt.datetime | None = Query(default=None),
                            SalesPlan: UploadFile = File(..., description="План продаж"),
                            CashFlow: UploadFile = File(..., description="ДДС"),
                            Margin: UploadFile = File(..., description="Маржа"),
                            date_from: dt.datetime = None,
                            date_to: dt.datetime = None):
    # try:
        # Считываем содержимое файла в байтовый буфер
        content1 = io.BytesIO(await SalesPlan.read())
        content2 = io.BytesIO(await CashFlow.read())
        content3 = io.BytesIO(await Margin.read())
        
            
        if date_from != None and date_to != None:
            delta = dt.timedelta(days=1)
            current_date = date_from
            while current_date <= date_to:
                content1.seek(0)
                content2.seek(0)
                content3.seek(0)
                data = await sales_data(current_date.date(), [content1, content2, content3])
                data_obj = await manual_write_data(current_date.date(), data)
                current_date += delta
        elif date != None:
            # Обрабатываем данные
            data = await sales_data(date.date(), [content1, content2, content3])
            data_obj = await manual_write_data(date, data)
            return {"status": "success", "message": "Data uploaded successfully", "data": data_obj}
        return {"status": "success", "message": "Data uploaded successfully"}
    # except Exception as ex:
    #     logger.error(f"Error processing file upload: {ex}")
    #     raise HTTPException(status_code=400, detail="Something went wrong!")

