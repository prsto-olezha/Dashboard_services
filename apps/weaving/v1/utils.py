from .models import Weaving_model
import dateutil.parser
from core.logger import logger
import datetime
from .models import ItemData
import core.backend as backend

# ================================


async def weaving_stats_aggregation(date_from, date_to, skip, limit, year:int = None, month:int = None):
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
                    "id": {"$last": "$_id"},
                    "created_at": {"$last": "$created_at"},
                    "date": {"$last": "$date"},
                    "fullness": {"$last": "$fullness"},
                    "pnevmat": {"$last": "$pnevmat"},
                    "rapira": {"$last": "$rapira"},
                    "total": {"$last": "$total"},
                }
            },
            {
                "$project": {
                    "_id": "$id",
                    "created_at": 1,
                    "date": 1,
                    "fullness": 1,
                    "pnevmat": 1,
                    "rapira": 1,
                    "total": 1,
                }
            },
            {"$facet": {"result": [{"$skip": skip}, {"$limit": limit}]}},
            {"$project": {"result": 1, "total": {"$size": "$result"}}},
        ]
        result = await Weaving_model.aggregate(aggregation_pipeline=pipeline).to_list(
            length=None
        )
        return result[0]
    except Exception as ex:
        logger.error(ex)


async def sum_weaving_stats_aggregation(date_from, date_to, year:int = None, month:int = None):
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
                    "date": {"$last": "$date"},
                    "pnevmat": {"$last": "$pnevmat"},
                    "rapira": {"$last": "$rapira"},
                    "total": {"$last": "$total"},
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
                    "pnevmat_sum": [
                        {
                            "$group": {
                                "_id": 0,
                                "plan": {"$sum": "$pnevmat.plan_total"},
                                "oper": {"$sum": "$pnevmat.oper_total"},
                                "fact": {"$sum": "$pnevmat.fact_total"},
                                "proc_plan": {"$sum": "$pnevmat.proc_plan"},
                            }
                        }
                    ],
                    "rapira_sum": [
                        {
                            "$group": {
                                "_id": 0,
                                "plan": {"$sum": "$rapira.plan_total"},
                                "oper": {"$sum": "$rapira.oper_total"},
                                "fact": {"$sum": "$rapira.fact_total"},
                                "proc_plan": {"$sum": "$rapira.proc_plan"},
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
                                "proc_plan": {"$sum": "$total.proc_plan"},
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
                            "pnevmat": {
                                "plan": {"$arrayElemAt": ["$pnevmat_sum.plan", 0]},
                                "oper": {"$arrayElemAt": ["$pnevmat_sum.oper", 0]},
                                "fact": {"$arrayElemAt": ["$pnevmat_sum.fact", 0]},
                                "proc_plan": {
                                    "$cond": [
                                        {
                                            "$gt": [
                                                {
                                                    "$arrayElemAt": [
                                                        "$pnevmat_sum.plan",
                                                        0,
                                                    ]
                                                },
                                                0,
                                            ]
                                        },
                                        {
                                            "$divide": [
                                                {
                                                    "$arrayElemAt": [
                                                        "$pnevmat_sum.fact",
                                                        0,
                                                    ]
                                                },
                                                {
                                                    "$divide": [
                                                        {
                                                            "$arrayElemAt": [
                                                                "$pnevmat_sum.plan",
                                                                0,
                                                            ]
                                                        },
                                                        100,
                                                    ]
                                                },
                                            ]
                                        },
                                        0,
                                    ]
                                },
                            },
                            "rapira": {
                                "plan": {"$arrayElemAt": ["$rapira_sum.plan", 0]},
                                "oper": {"$arrayElemAt": ["$rapira_sum.oper", 0]},
                                "fact": {"$arrayElemAt": ["$rapira_sum.fact", 0]},
                                "proc_plan": {
                                    "$cond": [
                                        {
                                            "$gt": [
                                                {
                                                    "$arrayElemAt": [
                                                        "$rapira_sum.plan",
                                                        0,
                                                    ]
                                                },
                                                0,
                                            ]
                                        },
                                        {
                                            "$divide": [
                                                {
                                                    "$arrayElemAt": [
                                                        "$rapira_sum.fact",
                                                        0,
                                                    ]
                                                },
                                                {
                                                    "$divide": [
                                                        {
                                                            "$arrayElemAt": [
                                                                "$rapira_sum.plan",
                                                                0,
                                                            ]
                                                        },
                                                        100,
                                                    ]
                                                },
                                            ]
                                        },
                                        0,
                                    ]
                                },
                            },
                            "total": {
                                "plan": {"$arrayElemAt": ["$total_sum.plan", 0]},
                                "oper": {"$arrayElemAt": ["$total_sum.oper", 0]},
                                "fact": {"$arrayElemAt": ["$total_sum.fact", 0]},
                                "proc_plan": {
                                    "$cond": [
                                        {
                                            "$gt": [
                                                {
                                                    "$arrayElemAt": [
                                                        "$total_sum.plan",
                                                        0,
                                                    ]
                                                },
                                                0,
                                            ]
                                        },
                                        {
                                            "$divide": [
                                                {
                                                    "$arrayElemAt": [
                                                        "$total_sum.fact",
                                                        0,
                                                    ]
                                                },
                                                {
                                                    "$divide": [
                                                        {
                                                            "$arrayElemAt": [
                                                                "$total_sum.plan",
                                                                0,
                                                            ]
                                                        },
                                                        100,
                                                    ]
                                                },
                                            ]
                                        },
                                        0,
                                    ]
                                },
                            },
                        }
                    ]
                }
            },
        ]
        result = await Weaving_model.aggregate(aggregation_pipeline=pipeline).to_list(
            length=None
        )
        return result[0]
    except Exception as ex:
        logger.error(ex)
    
async def manual_write_data(date_value: datetime.date, data):
    weaving_obj = Weaving_model(
        date=date_value,
        fullness = data["fullness"],
        rapira=ItemData(**data["rapira"]),
        pnevmat=ItemData(**data["pnevmat"]),
        total=ItemData(**data["total"]),
    )
    data_obj = await Weaving_model.insert_one(weaving_obj)
    logger.info("Weaving: Данные успешно записаны!")
    return data_obj

async def write_data():
    date_value = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)).date()
    data = await backend.weaving_data(date_value)
    weaving_obj = Weaving_model(
        date=date_value,
        fullness = data["fullness"],
        rapira=ItemData(**data["rapira"]),
        pnevmat=ItemData(**data["pnevmat"]),
        total=ItemData(**data["total"]),
    )
    await Weaving_model.insert_one(weaving_obj)
    logger.info("Weaving: Данные успешно записаны!")
