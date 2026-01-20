from .models import EcomDeviation_model, ItemData, TableData
import dateutil.parser
from core.logger import logger
import datetime
import core.backend as backend
# ==========================


async def ecom_deviation_stats_aggregation(date_from, date_to, skip, limit, year:int = None, month:int = None):
    try:
        if year == None and month == None:
            year = datetime.datetime.now(datetime.timezone.utc).year
            month = datetime.datetime.now(datetime.timezone.utc).month
            match_conditions = {
            "$and": [
                {"$eq": [{"$year": "$date"}, year]},
                {"$eq": [{"$month": "$date"}, month]}
            ]
                            }
        elif year != None and month == None:
            match_conditions = {
            "$and": [
                {"$eq": [{"$year": "$date"}, year]},
            ]
                            }
        elif year == None and month != None:
            match_conditions = {
            "$and": [
                {"$eq": [{"$month": "$date"}, month]}
            ]
                            }
        else:
            match_conditions = {
            "$and": [
                {"$eq": [{"$year": "$date"}, year]},
                {"$eq": [{"$month": "$date"}, month]}
            ]
                            }
        pipeline = [
            {
                "$match": {
                    "date": {
                        "$gte": dateutil.parser.parse(f"{date_from}"),
                        "$lte": dateutil.parser.parse(f"{date_to}"),
                    },
                    # "$expr": match_conditions
                }
            },
            {
                "$group": {
                    "_id": "$date",
                    "id": {"$last": "$_id"},
                    "created_at": {"$last": "$created_at"},
                    "date": {"$last": "$date"},
                    "sales": {"$last": "$sales"},
                    "orders": {"$last": "$orders"},
                    "table": {"$last": "$table"},
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
                    "orders": 1,
                    "table": 1,
                    "fullness": 1,
                    "fullness_date": 1,
                    "fullness_descriptions": 1,
                }
            },
            {"$facet": {"result": [{"$skip": skip}, {"$limit": limit}]}},
            {"$project": {"result": 1, "total": {"$size": "$result"}}},
        ]
        result = await EcomDeviation_model.aggregate(
            aggregation_pipeline=pipeline
        ).to_list(length=None)
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
        result = await EcomDeviation_model.aggregate(pipeline).to_list(length=1)
        return result[0]["fullness_date"].date() if result else None  # Возвращаем только fullness_date
    
    except Exception as ex:
        logger.error(f"Ошибка в get_last_fullness_date: {ex}")
        return None


async def manual_write_data(date_value: datetime.date, data):
    ecom_deviation_obj = EcomDeviation_model(
        date=date_value,
        sales=ItemData(**data["sales"]),
        orders=ItemData(**data["orders"]),
        table=TableData(**data["table"]),
        fullness = data["fullness"],
        fullness_date = data["fullness_date"],
        fullness_descriptions = data["fullness_descriptions"],
    )
    await EcomDeviation_model.insert_one(ecom_deviation_obj)
    logger.info("Ecom_deviation: Данные успешно записаны!\n", f"'{ecom_deviation_obj}'")
    return ecom_deviation_obj

async def write_data():
    date_value = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)).date()
    data = await backend.e_com_deviation_data(date_value)
    ecom_deviation_obj = EcomDeviation_model(
        date=date_value,
        sales=ItemData(**data["sales"]),
        orders=ItemData(**data["orders"]),
        table=TableData(**data["table"]),
        fullness = data["fullness"],
        fullness_date = data["fullness_date"],
        fullness_descriptions = data["fullness_descriptions"],
    )
    await EcomDeviation_model.insert_one(ecom_deviation_obj)  
    logger.info(f"Ecom_deviation: Данные успешно записаны!")
