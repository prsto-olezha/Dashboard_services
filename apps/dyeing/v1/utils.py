from .models import Dyeing_model
import dateutil.parser
from core.logger import logger
import datetime
from .models import ItemData
from core.backend import dyeing_data

# ================================


async def dyeing_stats_aggregation(date_from, date_to, skip, limit, year:int = None, month:int = None):
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
                    "fullness": {"$last": "$fullness"},
                    "saver": {"$last": "$saver"},
                    "dmc1": {"$last": "$dmc1"},
                    "dmc2": {"$last": "$dmc2"},
                    "kusters1": {"$last": "$kusters1"},
                    "kusters2": {"$last": "$kusters2"},
                    "henriksen1": {"$last": "$henriksen1"},
                    "henriksen2": {"$last": "$henriksen2"},
                    "total": {"$last": "$total"}
                }
            },
            {
                "$project": {
                    "_id": "$id",
                    "created_at": 1,
                    "date": 1,
                    "fullness": 1,
                    "saver": 1,
                    "dmc1": 1,
                    "dmc2": 1,
                    "kusters1": 1,
                    "kusters2": 1,
                    "henriksen1": 1,
                    "henriksen2": 1,
                    "total": 1,
                }
            },
            {"$facet": {"result": [{"$skip": skip}, {"$limit": limit}]}},
            {"$project": {"result": 1, "total": {"$size": "$result"}}},
        ]
        result = await Dyeing_model.aggregate(aggregation_pipeline=pipeline).to_list(
            length=None
        )
        return result[0]
    except Exception as ex:
        logger.error(ex)


async def sum_dyeing_stats_aggregation(date_from, date_to, year:int = None, month:int = None):
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
                    "created_at": {"$last": "$created_at"},
                    "date": {"$last": "$date"},
                    "fullness": {"$last": "$fullness"},
                    "saver": {"$last": "$saver"},
                    "dmc1": {"$last": "$dmc1"},
                    "dmc2": {"$last": "$dmc2"},
                    "kusters1": {"$last": "$kusters1"},
                    "kusters2": {"$last": "$kusters2"},
                    "henriksen1": {"$last": "$henriksen1"},
                    "henriksen2": {"$last": "$henriksen2"},
                    "total": {"$last": "$total"}
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
                    "saver_sum": [
                        {
                            "$group": {
                                "_id": 0,
                                "plan": {"$sum": "$saver.plan_total"},
                                "oper": {"$sum": "$saver.oper_total"},
                                "fact": {"$sum": "$saver.fact_total"},
                            }
                        }
                    ],
                    "dmc1_sum": [
                        {
                            "$group": {
                                "_id": 0,
                                "plan": {"$sum": "$dmc1.plan_total"},
                                "oper": {"$sum": "$dmc1.oper_total"},
                                "fact": {"$sum": "$dmc1.fact_total"},
                            }
                        }
                    ],
                    "dmc2_sum": [
                        {
                            "$group": {
                                "_id": 0,
                                "plan": {"$sum": "$dmc2.plan_total"},
                                "oper": {"$sum": "$dmc2.oper_total"},
                                "fact": {"$sum": "$dmc2.fact_total"},
                            }
                        }
                    ],
                    "kusters1_sum": [
                        {
                            "$group": {
                                "_id": 0,
                                "plan": {"$sum": "$kusters1.plan_total"},
                                "oper": {"$sum": "$kusters1.oper_total"},
                                "fact": {"$sum": "$kusters1.fact_total"},
                            }
                        }
                    ],
                    "kusters2_sum": [
                        {
                            "$group": {
                                "_id": 0,
                                "plan": {"$sum": "$kusters2.plan_total"},
                                "oper": {"$sum": "$kusters2.oper_total"},
                                "fact": {"$sum": "$kusters2.fact_total"},
                            }
                        }
                    ],
                    "henriksen1_sum": [
                        {
                            "$group": {
                                "_id": 0,
                                "plan": {"$sum": "$henriksen1.plan_total"},
                                "oper": {"$sum": "$henriksen1.oper_total"},
                                "fact": {"$sum": "$henriksen1.fact_total"},
                            }
                        }
                    ],
                    "henriksen2_sum": [
                        {
                            "$group": {
                                "_id": 0,
                                "plan": {"$sum": "$henriksen2.plan_total"},
                                "oper": {"$sum": "$henriksen2.oper_total"},
                                "fact": {"$sum": "$henriksen2.fact_total"},
                            }
                        }
                    ],
                    "total_sum": [
                        {
                            "$group": {
                                "_id": 0,
                                "plan": {"$sum": "$total.plan_total"},
                                "oper": {"$sum": "$total.oper_total"},
                                "fact": {"$sum": "$total.fact_total"},
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
                            "saver": {
                                "plan": {"$arrayElemAt": ["$saver_sum.plan", 0]},
                                "oper": {"$arrayElemAt": ["$saver_sum.oper", 0]},
                                "fact": {"$arrayElemAt": ["$saver_sum.fact", 0]},
                            },
                            "dmc1": {
                                "plan": {"$arrayElemAt": ["$dmc1_sum.plan", 0]},
                                "oper": {"$arrayElemAt": ["$dmc1_sum.oper", 0]},
                                "fact": {"$arrayElemAt": ["$dmc1_sum.fact", 0]},
                            },
                            "dmc2": {
                                "plan": {"$arrayElemAt": ["$dmc2_sum.plan", 0]},
                                "oper": {"$arrayElemAt": ["$dmc2_sum.oper", 0]},
                                "fact": {"$arrayElemAt": ["$dmc2_sum.fact", 0]},
                            },
                            "kusters1": {
                                "plan": {"$arrayElemAt": ["$kusters1_sum.plan", 0]},
                                "oper": {"$arrayElemAt": ["$kusters1_sum.oper", 0]},
                                "fact": {"$arrayElemAt": ["$kusters1_sum.fact", 0]},
                            },
                            "kusters2": {
                                "plan": {"$arrayElemAt": ["$kusters2_sum.plan", 0]},
                                "oper": {"$arrayElemAt": ["$kusters2_sum.oper", 0]},
                                "fact": {"$arrayElemAt": ["$kusters2_sum.fact", 0]},
                            },
                            "henriksen1": {
                                "plan": {"$arrayElemAt": ["$henriksen1_sum.plan", 0]},
                                "oper": {"$arrayElemAt": ["$henriksen1_sum.oper", 0]},
                                "fact": {"$arrayElemAt": ["$henriksen1_sum.fact", 0]},
                            },
                            "henriksen2": {
                                "plan": {"$arrayElemAt": ["$henriksen2_sum.plan", 0]},
                                "oper": {"$arrayElemAt": ["$henriksen2_sum.oper", 0]},
                                "fact": {"$arrayElemAt": ["$henriksen2_sum.fact", 0]},
                            },
                            "total": {
                                "plan": {"$arrayElemAt": ["$total_sum.plan", 0]},
                                "oper": {"$arrayElemAt": ["$total_sum.oper", 0]},
                                "fact": {"$arrayElemAt": ["$total_sum.fact", 0]},
                            },
                            
                        }
                    ]
                }
            },
        ]
        result = await Dyeing_model.aggregate(aggregation_pipeline=pipeline).to_list(
            length=None
        )
        return result[0]
    except Exception as ex:
        logger.error(ex)

async def manual_write_data(date_value: datetime.date, data):
    dyeing_obj = Dyeing_model(
        date=date_value,
        fullness = data["fullness"],
        saver = ItemData(**data["saver"]),
        dmc1 = ItemData(**data["dmc1"]),
        dmc2 = ItemData(**data["dmc2"]),
        kusters1 = ItemData(**data["kusters1"]),
        kusters2 = ItemData(**data["kusters2"]),
        henriksen1 = ItemData(**data["henriksen1"]),
        henriksen2 = ItemData(**data["henriksen2"]),
        total = ItemData(**data["total"]),
    )
    await Dyeing_model.insert_one(dyeing_obj)
    logger.info("dyeing: Данные успешно записаны!")
    return dyeing_obj

async def write_data():
    date_value = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)).date()
    data = await dyeing_data(date_value)
    dyeing_obj = Dyeing_model(
        date=date_value,
        fullness = data["fullness"],
        saver = ItemData(**data["saver"]),
        dmc1 = ItemData(**data["dmc1"]),
        dmc2 = ItemData(**data["dmc2"]),
        kusters1 = ItemData(**data["kusters1"]),
        kusters2 = ItemData(**data["kusters2"]),
        henriksen1 = ItemData(**data["henriksen1"]),
        henriksen2 = ItemData(**data["henriksen2"]),
        total = ItemData(**data["total"]),
    )
    await Dyeing_model.insert_one(dyeing_obj)
    logger.info("dyeing: Данные успешно записаны!")
