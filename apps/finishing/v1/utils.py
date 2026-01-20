from .models import Finishing_model
import dateutil.parser
from core.logger import logger
import datetime
from .models import ItemData
from core.backend import finishing_data

# ================================


async def finishing_stats_aggregation(date_from, date_to, skip, limit):
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
                    "id": {"$last": "$_id"},
                    "created_at": {"$last": "$created_at"},
                    "fullness": {"$last": "$fullness"},
                    "dyed": {"$last": "$dyed"},
                    "bleached": {"$last": "$bleached"},
                    "multicolored": {"$last": "$multicolored"},
                    "stamp": {"$last": "$stamp"},
                    "washed": {"$last": "$washed"},
                    "raw": {"$last": "$raw"},
                    "total": {"$last": "$total"},
                }
            },
            {
                "$project": {
                    "_id": "$id",
                    "created_at": 1,
                    "fullness": 1,
                    "dyed": 1,
                    "bleached": 1,
                    "multicolored": 1,
                    "stamp": 1,
                    "washed": 1,
                    "raw": 1,
                    "total": 1,
                }
            },
            {"$facet": {"result": [{"$skip": skip}, {"$limit": limit}]}},
            {"$project": {"result": 1, "total": {"$size": "$result"}}},
        ]
        result = await Finishing_model.aggregate(aggregation_pipeline=pipeline).to_list(
            length=None
        )
        return result[0]
    except Exception as ex:
        logger.error(ex)


async def sum_finishing_stats_aggregation(date_from, date_to):
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
                    "dyed": {"$last": "$dyed"},
                    "bleached": {"$last": "$bleached"},
                    "multicolored": {"$last": "$multicolored"},
                    "stamp": {"$last": "$stamp"},
                    "washed": {"$last": "$washed"},
                    "raw": {"$last": "$raw"},
                    "total": {"$last": "$total"},
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
                    'dyed': [
                        {"$sort": {"created_at": 1}},
                        {   
                            '$group': {
                                '_id': 0, 
                                'dyed': {
                                    '$last': '$dyed'
                                }
                            }
                        }
                    ],
                    "bleached": [
                        {"$sort": {"created_at": 1}},
                        {   
                            '$group': {
                                '_id': 0, 
                                'bleached': {
                                    '$last': '$bleached'
                                }
                            }
                        }
                    ],
                    "multicolored": [
                        {"$sort": {"created_at": 1}},
                        {   
                            '$group': {
                                '_id': 0, 
                                'multicolored': {
                                    '$last': '$multicolored'
                                }
                            }
                        }
                    ],
                    "stamp": [
                        {"$sort": {"created_at": 1}},
                        {   
                            '$group': {
                                '_id': 0, 
                                'stamp': {
                                    '$last': '$stamp'
                                }
                            }
                        }
                    ],
                    "washed": [
                        {"$sort": {"created_at": 1}},
                        {   
                            '$group': {
                                '_id': 0, 
                                'washed': {
                                    '$last': '$washed'
                                }
                            }
                        }
                    ],
                    "raw": [
                        {"$sort": {"created_at": 1}},
                        {   
                            '$group': {
                                '_id': 0, 
                                'raw': {
                                    '$last': '$raw'
                                }
                            }
                        }
                    ],
                    "total": [
                        {"$sort": {"created_at": 1}},
                        {   
                            '$group': {
                                '_id': 0, 
                                'total': {
                                    '$last': '$total'
                                }
                            }
                        }
                    ]
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
                            'dyed': {
                                '$arrayElemAt': ['$dyed.dyed', 0]
                            }, 
                            'bleached': {
                                '$arrayElemAt': ['$bleached.bleached', 0]
                            }, 
                            'multicolored': {
                                '$arrayElemAt': ['$multicolored.multicolored', 0]
                            },
                            'stamp': {
                                '$arrayElemAt': ['$stamp.stamp', 0]
                            },
                            'washed': {
                                '$arrayElemAt': ['$washed.washed', 0]
                            },
                            'raw': {
                                '$arrayElemAt': ['$raw.raw', 0]
                            },
                            'total': {
                                '$arrayElemAt': ['$total.total', 0]
                            },
                        }
                    ]
                }
            },
        ]
        result = await Finishing_model.aggregate(aggregation_pipeline=pipeline).to_list(
            length=None
        )
        return result[0]
    except Exception as ex:
        logger.error(ex)


async def manual_write_data(data):
    finishing_obj = Finishing_model(
        fullness=data["fullness"],
        dyed=ItemData(**data["dyed"]),
        bleached=ItemData(**data["bleached"]),
        multicolored=ItemData(**data["multicolored"]),
        stamp=ItemData(**data["stamp"]),
        washed=ItemData(**data["washed"]),
        raw=ItemData(**data["raw"]),
        total=ItemData(**data["total"]),
    )
    await Finishing_model.insert_one(finishing_obj)
    logger.info("finishing: Данные успешно записаны!")
    return finishing_obj


async def write_data():
    data = await finishing_data()
    finishing_obj = Finishing_model(
        fullness=data["fullness"],
        dyed=ItemData(**data["dyed"]),
        bleached=ItemData(**data["bleached"]),
        multicolored=ItemData(**data["multicolored"]),
        stamp=ItemData(**data["stamp"]),
        washed=ItemData(**data["washed"]),
        raw=ItemData(**data["raw"]),
        total=ItemData(**data["total"]),
    )
    await Finishing_model.insert_one(finishing_obj)
    logger.info("finishing: Данные успешно записаны!")
