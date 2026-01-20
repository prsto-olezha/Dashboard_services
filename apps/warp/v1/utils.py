from .models import Warp_model
import dateutil.parser
from core.logger import logger
import datetime
from .models import ItemData
from core.backend import warp_data

# ================================


async def warp_stats_aggregation(date_from, date_to, skip, limit, year:int = None, month:int = None):
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
                    "created_at": {
                        "$gte": dateutil.parser.parse(f"{date_from}"),
                        "$lte": dateutil.parser.parse(f"{date_to}"),
                    },
                    "$expr": match_conditions
                }
            },
            {
                "$group": {
                    "_id": "$date",
                    "created_at": {"$last": "$created_at"},
                    "date": {"$last": "$date"},
                    "fullness": {"$last": "$fullness"},
                    "cotton": {"$last": "$cotton"},
                    "aramid": {"$last": "$aramid"},
                    "linen": {"$last": "$linen"},
                }
            },
            {
                "$project": {
                    "_id": "$id",
                    "created_at": 1,
                    "date": 1,
                    "cotton": 1,
                    "aramid": 1,
                    "linen": 1,   
                }
            },
            {"$facet": {"result": [{"$skip": skip}, {"$limit": limit}]}},
            {"$project": {"result": 1, "total": {"$size": "$result"}}},
        ]
        result = await Warp_model.aggregate(aggregation_pipeline=pipeline).to_list(
            length=None
        )
        return result[0]
    except Exception as ex:
        logger.error(ex)


async def sum_warp_stats_aggregation(date_from, date_to, year:int = None, month:int = None):
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
                    "created_at": {
                        "$gte": dateutil.parser.parse(f"{date_from}"),
                        "$lte": dateutil.parser.parse(f"{date_to}"),
                    },
                    "$expr": match_conditions
                    }
                },
            {
                "$group": {
                    "_id": "$date",
                    "created_at": {"$last": "$created_at"},
                    "fullness": {"$last": "$fullness"},
                    "cotton": {"$last": "$cotton"},
                    "aramid": {"$last": "$aramid"},
                    "linen": {"$last": "$linen"},
                }
            },
            {
                "$facet": {
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
                    'cotton_total': [
                        {   
                            '$group': {
                                '_id': 0, 
                                'count': {
                                    '$sum': '$cotton.total.count'
                                },
                                'meters': {
                                    '$sum': '$cotton.total.meters'
                                },
                                'break_count': {
                                    '$sum': '$cotton.total.break_count'
                                },
                            }
                        }
                    ],
                    'aramid_total': [
                        {   
                            '$group': {
                                '_id': 0, 
                                'count': {
                                    '$sum': '$aramid.total.count'
                                },
                                'meters': {
                                    '$sum': '$aramid.total.meters'
                                },
                                'break_count': {
                                    '$sum': '$aramid.total.break_count'
                                },
                            }
                        }
                    ],
                    'linen_total': [
                        {   
                            '$group': {
                                '_id': 0, 
                                'count': {
                                    '$sum': '$linen.total.count'
                                },
                                'meters': {
                                    '$sum': '$linen.total.meters'
                                },
                                'break_count': {
                                    '$sum': '$linen.total.break_count'
                                },
                            }
                        }
                    ],
                }
            },
            {
                "$project": {
                    "result": [
                        {
                            'fullness': {
                                '$arrayElemAt': ['$fullness.fullness', 0]
                            },
                            'cotton': {
                                "total": {
                                    "count": {'$arrayElemAt': ['$cotton_total.count', 0]},
                                    "meters": {'$arrayElemAt': ['$cotton_total.meters', 0]},
                                    "break_count": {'$arrayElemAt': ['$cotton_total.break_count', 0]},
                                }
                            },
                            'aramid': {
                                "total": {
                                    "count": {'$arrayElemAt': ['$aramid_total.count', 0]},
                                    "meters": {'$arrayElemAt': ['$aramid_total.meters', 0]},
                                    "break_count": {'$arrayElemAt': ['$aramid_total.break_count', 0]},
                                }
                            },
                            'linen': {
                                "total": {
                                    "count": {'$arrayElemAt': ['$linen_total.count', 0]},
                                    "meters": {'$arrayElemAt': ['$linen_total.meters', 0]},
                                    "break_count": {'$arrayElemAt': ['$linen_total.break_count', 0]},
                                }
                            },
                        }
                    ]
                }
            }
        ]
        result = await Warp_model.aggregate(aggregation_pipeline=pipeline).to_list(
            length=None
        )
        return result[0]
    except Exception as ex:
        logger.error(ex)


async def manual_write_data(date_value: datetime.date, data):
    warp_obj = Warp_model(
        date = date_value,
        fullness = data["fullness"],
        cotton = {key: ItemData(**value) for key, value in data.get("cotton", {}).items()},
        aramid = {key: ItemData(**value) for key, value in data.get("aramid", {}).items()},
        linen = {key: ItemData(**value) for key, value in data.get("linen", {}).items()},
    )
    await Warp_model.insert_one(warp_obj)
    logger.info(f"Warp: Данные за {date_value} успешно записаны!")
    return warp_obj


async def write_data():
    date_value = (
        datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)
    ).date()
    data = await warp_data(date_value)
    warp_obj = Warp_model(
        date = date_value,
        fullness = data["fullness"],
        cotton = {key: ItemData(**value) for key, value in data.get("cotton", {}).items()},
        aramid = {key: ItemData(**value) for key, value in data.get("aramid", {}).items()},
        linen = {key: ItemData(**value) for key, value in data.get("linen", {}).items()},
    )
    await Warp_model.insert_one(warp_obj)
    logger.info("Warp: Данные успешно записаны!")
