from .models import Sewing_model
import dateutil.parser
from core.logger import logger
import datetime
from .models import ItemData, product, production
from core.backend import sewing_data

# ================================


async def sewing_stats_aggregation(date_from, date_to, skip, limit):
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
                    "created_at": {"$last": "$created_at"},
                    "date": {"$last": "$date"},
                    "fullness": {"$last": "$fullness"},
                    "kpb": {"$last": "$kpb"},
                    "rub": {"$last": "$rub"},
                }
            },
            {
                "$project": {
                    "_id": "$id",
                    "created_at": 1,
                    "date": 1,
                    "kpb": 1,
                    "rub": 1,          
                }
            },
            {"$facet": {"result": [{"$skip": skip}, {"$limit": limit}]}},
            {"$project": {"result": 1, "total": {"$size": "$result"}}},
        ]
        result = await Sewing_model.aggregate(aggregation_pipeline=pipeline).to_list(
            length=None
        )
        return result[0]
    except Exception as ex:
        logger.error(ex)


async def sum_sewing_stats_aggregation(date_from, date_to):
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
                    "_id": "$created_at",
                    "created_at": {"$last": "$created_at"},
                    "fullness": {"$last": "$fullness"},
                    "kpb": {"$last": "$kpb"},
                    "rub": {"$last": "$rub"},
                }
            },
            {
                "$facet": {
                    'created_at': [
                        {"$sort": {"created_at": 1}},
                        {   
                            '$group': {
                                '_id': 0, 
                                'created_at': {
                                    '$last': '$created_at'
                                }
                            }
                        }
                    ],
                    'fullness': [
                        {"$sort": {"created_at": 1}},
                        {   
                            '$group': {
                                '_id': 0, 
                                'fullness': {
                                    '$last': '$fullness'
                                }
                            }
                        }
                    ],
                    'kpb': [
                        {"$sort": {"created_at": 1}},
                        {   
                            '$group': {
                                '_id': 0, 
                                'kpb': {
                                    '$last': '$kpb'
                                }
                            }
                        }
                    ],
                    'rub': [
                        {"$sort": {"created_at": 1}},
                        {   
                            '$group': {
                                '_id': 0, 
                                'rub': {
                                    '$last': '$rub'
                                }
                            }
                        }
                    ],
                }
            },
            {
                "$project": {
                    "result": [
                        {
                            'created_at': {
                                '$arrayElemAt': ['$created_at.created_at', 0]
                            },
                            'fullness': {
                                '$arrayElemAt': ['$fullness.fullness', 0]
                            },
                            'kpb': {
                                '$arrayElemAt': ['$kpb.kpb', 0]
                            }, 
                            'rub': {
                                '$arrayElemAt': ['$rub.rub', 0]
                            }, 
                        }
                    ]
                }
            },
        ]
        result = await Sewing_model.aggregate(aggregation_pipeline=pipeline).to_list(
            length=None
        )
        return result[0]
    except Exception as ex:
        logger.error(ex)


async def manual_write_data(data):
    date_value = (
        datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)
    ).date()
    sewing_obj = Sewing_model(
        date = date_value,
        fullness = data["fullness"],
        kpb = production(**data["kpb"]),
        rub = production(**data["rub"]),
    )
    await Sewing_model.insert_one(sewing_obj)
    logger.info("Sewing: Данные успешно записаны!")
    return sewing_obj


async def write_data():
    date_value = (
        datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)
    ).date()
    data = await sewing_data()
    sewing_obj = Sewing_model(
        date = date_value,
        fullness = data["fullness"],
        kpb = production(**data["kpb"]),
        rub = production(**data["rub"]),
    )
    await Sewing_model.insert_one(sewing_obj)
    logger.info("Sewing: Данные успешно записаны!")
