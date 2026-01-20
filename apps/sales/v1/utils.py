from .models import Sales_model, SalesColumn, CashFlowColumn
import dateutil.parser
from core.logger import logger
import datetime
import core.backend as backend
# ================================

async def sales_stats_aggregation(date_from, date_to, skip, limit):
    try:
        pipeline = [
            {
                "$match": {
                    "date": {
                        "$gte": dateutil.parser.parse(f"{date_from}"),
                        "$lte": dateutil.parser.parse(f"{date_to}"),
                    },
                    # "fullness": True
                }
            },
            # {
            #     "$sort": {
            #         "fullness_date": 1,  # Сортировка по дате (по возрастанию)
            #         "created_at": -1  # Сортировка по created_at (по убыванию)
            #     }
            # },
            {
                "$group": {
                    "_id": "$date",
                    "id": {"$last": "$_id"},
                    "created_at": {"$last": "$created_at"},
                    "date": {"$last": "$date"},
                    "sales": {"$last": "$sales"},
                    "cash_flow": {"$last": "$cash_flow"},
                    "fullness": {"$last": "$fullness"},
                    "fullness_date": {"$last": "$fullness_date"},
                    "fullness_descriptions": {"$last": "$fullness_descriptions"},
                }
            },
            {
                "$project": {
                    "_id": "$id",
                    "created_at": 1,
                    "date": 1,
                    "sales": 1,
                    "cash_flow": 1,
                    "fullness": 1,
                    "fullness_date": 1,
                    "fullness_descriptions": 1,
                }
            },
            {"$facet": {"result": [{"$skip": skip}, {"$limit": limit}]}},
            {"$project": {"result": 1, "total": {"$size": "$result"}}},
        ]
        result = await Sales_model.aggregate(aggregation_pipeline=pipeline).to_list(
            length=None
        )
        return result[0]
    except Exception as ex:
        logger.error(ex)
        
        
async def get_last_fullness_date():
    try:
        pipeline = [
            # Фильтруем записи, где fullness_date не равен null
            {"$match": {"fullness_date": {"$ne": None}}},
            # Сортируем по убыванию (новейшая дата первой)
            {"$sort": {"fullness_date": -1}},
            # Берём только одну запись
            {"$limit": 1},
            # Оставляем только поле fullness_date
            {"$project": {"_id": 0, "fullness_date": 1}}
        ]
        result = await Sales_model.aggregate(pipeline).to_list(length=1)
        return result[0]["fullness_date"].date() if result else None  # Возвращаем только fullness_date
    
    except Exception as ex:
        logger.error(f"Ошибка в get_last_fullness_date: {ex}")
        return None
    
    
async def manual_write_data(date, data):
    sales_obj = Sales_model(
        date = date,
        sales = SalesColumn(**data["sales"]),
        cash_flow = CashFlowColumn(**data["cash_flow"]),
        fullness = data["fullness"],
        fullness_date = data["fullness_date"],
        fullness_descriptions = data["fullness_descriptions"],
    )
    await Sales_model.insert_one(sales_obj)
    logger.info("sales: Данные успешно записаны!")
    return sales_obj


async def write_data():
    date_value = (
        datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)
    ).date()
    data = await backend.sales_data(date_value)
    sales_obj = Sales_model(
        date = date_value,
        sales = SalesColumn(**data["sales"]),
        cash_flow = CashFlowColumn(**data["cash_flow"]),
        fullness = data["fullness"],
        fullness_date = data["fullness_date"],
        fullness_descriptions = data["fullness_descriptions"],
    )
    await Sales_model.insert_one(sales_obj)
    logger.info("sales: Данные успешно записаны!")
