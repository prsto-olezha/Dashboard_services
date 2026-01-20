from .models import Sales_plan_model
import dateutil.parser
from core.logger import logger
import datetime
from .models import ItemData, Section
from core.backend import sales_plan_data
# ================================


async def sales_plan_stats_aggregation(date_from, date_to, skip, limit):
    try:
        pipeline = [
            {
                "$match": {
                    "created_at": {
                        "$gte": dateutil.parser.parse(f"{date_from}"),
                        "$lte": dateutil.parser.parse(f"{date_to}"),
                    }
                }
            },
            {
                "$group": {
                    "_id": "$date",
                    "id": {"$last": "$_id"},
                    "created_at": {"$last": "$created_at"},
                    "shipments": {"$last": "$shipments"},
                    "receipts": {"$last": "$receipts"},
                    "fullness": {"$last": "$fullness"},
                }
            },
            {
                "$project": {
                    "_id": "$id",
                    "created_at": 1,
                    "shipments": 1,
                    "receipts": 1,
                    "fullness": 1,
                }
            },
            {"$facet": {"result": [{"$skip": skip}, {"$limit": limit}]}},
            {"$project": {"result": 1, "total": {"$size": "$result"}}},
        ]
        result = await Sales_plan_model.aggregate(aggregation_pipeline=pipeline).to_list(
            length=None
        )
        return result[0]
    except Exception as ex:
        logger.error(ex)

async def manual_write_data(data):
    sales_plan_obj = Sales_plan_model(
        shipments = Section(**data["shipments"]),
        receipts = Section(**data["receipts"]),
        fullness = data["fullness"],
    )
    await Sales_plan_model.insert_one(sales_plan_obj)
    logger.info("sales_plan: Данные успешно записаны!")
    return sales_plan_obj

async def write_data():
    data = await sales_plan_data()
    sales_plan_obj = Sales_plan_model(
        shipments = Section(**data["shipments"]),
        receipts = Section(**data["receipts"]),
        fullness = data["fullness"],
    )
    await Sales_plan_model.insert_one(sales_plan_obj)
    logger.info("sales_plan: Данные успешно записаны!")
