from .models import MarketplacesEcom, Items
import dateutil.parser
from core.logger import logger
import datetime
from core.backend import e_com_data
# ==========================


async def marketplaces_stats_aggregation(date_from, date_to, skip, limit, year:int = None, month:int = None):
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
                    "sales": {"$last": "$sales"},
                    "orders": {"$last": "$orders"},
                }
            },
            {
                "$project": {
                    "_id": "$id",
                    "created_at": 1,
                    "date": 1,
                    "fullness": 1,
                    "sales": 1,
                    "orders": 1,
                    
                }
            },
            {"$facet": {"result": [{"$skip": skip}, {"$limit": limit}]}},
            {"$project": {"result": 1, "total": {"$size": "$result"}}},
        ]
        result = await MarketplacesEcom.aggregate(
            aggregation_pipeline=pipeline
        ).to_list(length=None)
        return result[0]
    except Exception as ex:
        logger.error(ex)


async def sum_marketplaces_stats_aggregation(date_from, date_to, year:int = None, month:int = None):
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
                '$group': {
                    '_id': '$date', 
                    'created_at': {
                        '$last': '$created_at'
                    }, 
                    'date': {
                        '$last': '$date'
                    }, 
                    'fullness': {
                        '$last': '$fullness'
                    }, 
                    'sales': {
                        '$last': '$sales'
                    }, 
                    'orders': {
                        '$last': '$orders'
                    }
                }
            }, {
                '$facet': {
                    'fullness': [
                        {"$sort": {"date": 1}},
                        {   
                            '$group': {
                                '_id': 0, 
                                'fullness': {
                                    '$last': '$fullness'
                                }
                            }
                        }
                    ], 
                    'month_plan_sales': [
                        {"$sort": {"date": 1}},
                        {   
                            '$group': {
                                '_id': 0, 
                                'month_plan': {
                                    '$last': '$sales.month_plan'
                                }
                            }
                        }
                    ], 
                    'month_oper_sales': [
                        {"$sort": {"date": 1}},
                        {   
                            '$group': {
                                '_id': 0, 
                                'month_oper': {
                                    '$last': '$sales.month_oper'
                                }
                            }
                        }
                    ], 
                    'ozon_sales': [
                        {
                            '$group': {
                                '_id': 0, 
                                'plan': {
                                    '$sum': '$sales.ozon.plan_total'
                                }, 
                                'oper': {
                                    '$sum': '$sales.ozon.oper_total'
                                }, 
                                'fact': {
                                    '$sum': '$sales.ozon.fact_total'
                                }, 
                                'proc_plan': {
                                    '$sum': '$sales.ozon.proc_plan'
                                }
                            }
                        }
                    ], 
                    'wb_sales': [
                        {
                            '$group': {
                                '_id': 0, 
                                'plan': {
                                    '$sum': '$sales.wb.plan_total'
                                }, 
                                'oper': {
                                    '$sum': '$sales.wb.oper_total'
                                }, 
                                'fact': {
                                    '$sum': '$sales.wb.fact_total'
                                }, 
                                'proc_plan': {
                                    '$sum': '$sales.wb.proc_plan'
                                }
                            }
                        }
                    ], 
                    'lamoda_sales': [
                        {
                            '$group': {
                                '_id': 0, 
                                'plan': {
                                    '$sum': '$sales.lamoda.plan_total'
                                }, 
                                'oper': {
                                    '$sum': '$sales.lamoda.oper_total'
                                }, 
                                'fact': {
                                    '$sum': '$sales.lamoda.fact_total'
                                }, 
                                'proc_plan': {
                                    '$sum': '$sales.lamoda.proc_plan'
                                }
                            }
                        }
                    ], 
                    'total_sales': [
                        {
                            '$group': {
                                '_id': 0, 
                                'plan': {
                                    '$sum': '$sales.total.plan_total'
                                }, 
                                'oper': {
                                    '$sum': '$sales.total.oper_total'
                                }, 
                                'fact': {
                                    '$sum': '$sales.total.fact_total'
                                }, 
                                'proc_plan': {
                                    '$sum': '$sales.total.proc_plan'
                                }
                            }
                        }
                    ], 
                   'month_plan_orders': [
                        {"$sort": {"date": 1}},
                        {   
                            '$group': {
                                '_id': 0, 
                                'month_plan': {
                                    '$last': '$orders.month_plan'
                                }
                            }
                        }
                    ], 
                    'month_oper_orders': [
                        {"$sort": {"date": 1}},
                        {   
                            '$group': {
                                '_id': 0, 
                                'month_oper': {
                                    '$last': '$orders.month_oper'
                                }
                            }
                        }
                    ], 
                    'ozon_orders': [
                        {
                            '$group': {
                                '_id': 0, 
                                'plan': {
                                    '$sum': '$orders.ozon.plan_total'
                                }, 
                                'oper': {
                                    '$sum': '$orders.ozon.oper_total'
                                }, 
                                'fact': {
                                    '$sum': '$orders.ozon.fact_total'
                                }, 
                                'proc_plan': {
                                    '$sum': '$orders.ozon.proc_plan'
                                }
                            }
                        }
                    ], 
                    'wb_orders': [
                        {
                            '$group': {
                                '_id': 0, 
                                'plan': {
                                    '$sum': '$orders.wb.plan_total'
                                }, 
                                'oper': {
                                    '$sum': '$orders.wb.oper_total'
                                }, 
                                'fact': {
                                    '$sum': '$orders.wb.fact_total'
                                }, 
                                'proc_plan': {
                                    '$sum': '$orders.wb.proc_plan'
                                }
                            }
                        }
                    ], 
                    'lamoda_orders': [
                        {
                            '$group': {
                                '_id': 0, 
                                'plan': {
                                    '$sum': '$orders.lamoda.plan_total'
                                }, 
                                'oper': {
                                    '$sum': '$orders.lamoda.oper_total'
                                }, 
                                'fact': {
                                    '$sum': '$orders.lamoda.fact_total'
                                }, 
                                'proc_plan': {
                                    '$sum': '$orders.lamoda.proc_plan'
                                }
                            }
                        }
                    ], 
                    'total_orders': [
                        {
                            '$group': {
                                '_id': 0, 
                                'plan': {
                                    '$sum': '$orders.total.plan_total'
                                }, 
                                'oper': {
                                    '$sum': '$orders.total.oper_total'
                                }, 
                                'fact': {
                                    '$sum': '$orders.total.fact_total'
                                }, 
                                'proc_plan': {
                                    '$sum': '$orders.total.proc_plan'
                                }
                            }
                        }
                    ]
                }
            }, {
                '$project': {
                    'result': [
                        {
                            'fullness': {
                                '$arrayElemAt': [
                                    '$fullness.fullness', 0
                                ]
                            }, 
                            'sales': {
                                'month_plan': {'$arrayElemAt': ['$month_plan_sales.month_plan', 0]},
                                'month_oper': {'$arrayElemAt': ['$month_oper_sales.month_oper', 0]},
                                'ozon': {
                                    'plan': {
                                        '$arrayElemAt': [
                                            '$ozon_sales.plan', 0
                                        ]
                                    }, 
                                    'oper': {
                                        '$arrayElemAt': [
                                            '$ozon_sales.oper', 0
                                        ]
                                    }, 
                                    'fact': {
                                        '$arrayElemAt': [
                                            '$ozon_sales.fact', 0
                                        ]
                                    }, 
                                    'proc_plan': {
                                        '$cond': [
                                            {
                                                '$gt': [
                                                    {
                                                        '$arrayElemAt': [
                                                            '$ozon_sales.plan', 0
                                                        ]
                                                    }, 0
                                                ]
                                            }, {
                                                '$divide': [
                                                    {
                                                        '$arrayElemAt': [
                                                            '$ozon_sales.fact', 0
                                                        ]
                                                    }, {
                                                        '$divide': [
                                                            {
                                                                '$arrayElemAt': [
                                                                    '$ozon_sales.plan', 0
                                                                ]
                                                            }, 100
                                                        ]
                                                    }
                                                ]
                                            }, 0
                                        ]
                                    }
                                }, 
                                'wb': {
                                    'plan': {
                                        '$arrayElemAt': [
                                            '$wb_sales.plan', 0
                                        ]
                                    }, 
                                    'oper': {
                                        '$arrayElemAt': [
                                            '$wb_sales.oper', 0
                                        ]
                                    }, 
                                    'fact': {
                                        '$arrayElemAt': [
                                            '$wb_sales.fact', 0
                                        ]
                                    }, 
                                    'proc_plan': {
                                        '$cond': [
                                            {
                                                '$gt': [
                                                    {
                                                        '$arrayElemAt': [
                                                            '$wb_sales.plan', 0
                                                        ]
                                                    }, 0
                                                ]
                                            }, {
                                                '$divide': [
                                                    {
                                                        '$arrayElemAt': [
                                                            '$wb_sales.fact', 0
                                                        ]
                                                    }, {
                                                        '$divide': [
                                                            {
                                                                '$arrayElemAt': [
                                                                    '$wb_sales.plan', 0
                                                                ]
                                                            }, 100
                                                        ]
                                                    }
                                                ]
                                            }, 0
                                        ]
                                    }
                                }, 
                                'lamoda': {
                                    'plan': {
                                        '$arrayElemAt': [
                                            '$lamoda_sales.plan', 0
                                        ]
                                    }, 
                                    'oper': {
                                        '$arrayElemAt': [
                                            '$lamoda_sales.oper', 0
                                        ]
                                    }, 
                                    'fact': {
                                        '$arrayElemAt': [
                                            '$lamoda_sales.fact', 0
                                        ]
                                    }, 
                                    'proc_plan': {
                                        '$cond': [
                                            {
                                                '$gt': [
                                                    {
                                                        '$arrayElemAt': [
                                                            '$lamoda_sales.plan', 0
                                                        ]
                                                    }, 0
                                                ]
                                            }, {
                                                '$divide': [
                                                    {
                                                        '$arrayElemAt': [
                                                            '$lamoda_sales.fact', 0
                                                        ]
                                                    }, {
                                                        '$divide': [
                                                            {
                                                                '$arrayElemAt': [
                                                                    '$lamoda_sales.plan', 0
                                                                ]
                                                            }, 100
                                                        ]
                                                    }
                                                ]
                                            }, 0
                                        ]
                                    }
                                }, 
                                'total': {
                                    'plan': {
                                        '$arrayElemAt': [
                                            '$total_sales.plan', 0
                                        ]
                                    }, 
                                    'oper': {
                                        '$arrayElemAt': [
                                            '$total_sales.oper', 0
                                        ]
                                    }, 
                                    'fact': {
                                        '$arrayElemAt': [
                                            '$total_sales.fact', 0
                                        ]
                                    }, 
                                    'proc_plan': {
                                        '$cond': [
                                            {
                                                '$gt': [
                                                    {
                                                        '$arrayElemAt': [
                                                            '$total_sales.plan', 0
                                                        ]
                                                    }, 0
                                                ]
                                            }, {
                                                '$divide': [
                                                    {
                                                        '$arrayElemAt': [
                                                            '$total_sales.fact', 0
                                                        ]
                                                    }, {
                                                        '$divide': [
                                                            {
                                                                '$arrayElemAt': [
                                                                    '$total_sales.plan', 0
                                                                ]
                                                            }, 100
                                                        ]
                                                    }
                                                ]
                                            }, 0
                                        ]
                                    }
                                }
                            }, 
                            'orders': {
                                'month_plan': {'$arrayElemAt': ['$month_plan_orders.month_plan', 0]},
                                'month_oper': {'$arrayElemAt': ['$month_oper_orders.month_oper', 0]},
                                'ozon': {
                                    'plan': {
                                        '$arrayElemAt': [
                                            '$ozon_orders.plan', 0
                                        ]
                                    }, 
                                    'oper': {
                                        '$arrayElemAt': [
                                            '$ozon_orders.oper', 0
                                        ]
                                    }, 
                                    'fact': {
                                        '$arrayElemAt': [
                                            '$ozon_orders.fact', 0
                                        ]
                                    }, 
                                    'proc_plan': {
                                        '$cond': [
                                            {
                                                '$gt': [
                                                    {
                                                        '$arrayElemAt': [
                                                            '$ozon_orders.plan', 0
                                                        ]
                                                    }, 0
                                                ]
                                            }, {
                                                '$divide': [
                                                    {
                                                        '$arrayElemAt': [
                                                            '$ozon_orders.fact', 0
                                                        ]
                                                    }, {
                                                        '$divide': [
                                                            {
                                                                '$arrayElemAt': [
                                                                    '$ozon_orders.plan', 0
                                                                ]
                                                            }, 100
                                                        ]
                                                    }
                                                ]
                                            }, 0
                                        ]
                                    }
                                }, 
                                'wb': {
                                    'plan': {
                                        '$arrayElemAt': [
                                            '$wb_orders.plan', 0
                                        ]
                                    }, 
                                    'oper': {
                                        '$arrayElemAt': [
                                            '$wb_orders.oper', 0
                                        ]
                                    }, 
                                    'fact': {
                                        '$arrayElemAt': [
                                            '$wb_orders.fact', 0
                                        ]
                                    }, 
                                    'proc_plan': {
                                        '$cond': [
                                            {
                                                '$gt': [
                                                    {
                                                        '$arrayElemAt': [
                                                            '$wb_orders.plan', 0
                                                        ]
                                                    }, 0
                                                ]
                                            }, {
                                                '$divide': [
                                                    {
                                                        '$arrayElemAt': [
                                                            '$wb_orders.fact', 0
                                                        ]
                                                    }, {
                                                        '$divide': [
                                                            {
                                                                '$arrayElemAt': [
                                                                    '$wb_orders.plan', 0
                                                                ]
                                                            }, 100
                                                        ]
                                                    }
                                                ]
                                            }, 0
                                        ]
                                    }
                                }, 
                                'lamoda': {
                                    'plan': {
                                        '$arrayElemAt': [
                                            '$lamoda_orders.plan', 0
                                        ]
                                    }, 
                                    'oper': {
                                        '$arrayElemAt': [
                                            '$lamoda_orders.oper', 0
                                        ]
                                    }, 
                                    'fact': {
                                        '$arrayElemAt': [
                                            '$lamoda_orders.fact', 0
                                        ]
                                    }, 
                                    'proc_plan': {
                                        '$cond': [
                                            {
                                                '$gt': [
                                                    {
                                                        '$arrayElemAt': [
                                                            '$lamoda_orders.plan', 0
                                                        ]
                                                    }, 0
                                                ]
                                            }, {
                                                '$divide': [
                                                    {
                                                        '$arrayElemAt': [
                                                            '$lamoda_orders.fact', 0
                                                        ]
                                                    }, {
                                                        '$divide': [
                                                            {
                                                                '$arrayElemAt': [
                                                                    '$lamoda_orders.plan', 0
                                                                ]
                                                            }, 100
                                                        ]
                                                    }
                                                ]
                                            }, 0
                                        ]
                                    }
                                }, 
                                'total': {
                                    'plan': {
                                        '$arrayElemAt': [
                                            '$total_orders.plan', 0
                                        ]
                                    }, 
                                    'oper': {
                                        '$arrayElemAt': [
                                            '$total_orders.oper', 0
                                        ]
                                    }, 
                                    'fact': {
                                        '$arrayElemAt': [
                                            '$total_orders.fact', 0
                                        ]
                                    }, 
                                    'proc_plan': {
                                        '$cond': [
                                            {
                                                '$gt': [
                                                    {
                                                        '$arrayElemAt': [
                                                            '$total_orders.plan', 0
                                                        ]
                                                    }, 0
                                                ]
                                            }, {
                                                '$divide': [
                                                    {
                                                        '$arrayElemAt': [
                                                            '$total_orders.fact', 0
                                                        ]
                                                    }, {
                                                        '$divide': [
                                                            {
                                                                '$arrayElemAt': [
                                                                    '$total_orders.plan', 0
                                                                ]
                                                            }, 100
                                                        ]
                                                    }
                                                ]
                                            }, 0
                                        ]
                                    }
                                }
                            }
                        }
                    ]
                }
            }
        ]
        result = await MarketplacesEcom.aggregate(
            aggregation_pipeline=pipeline
        ).to_list(length=None)
        return result[0]
    except Exception as ex:
        logger.error(ex)

async def manual_write_data(date_value: datetime.date, data):
    marketplaces_obj = MarketplacesEcom(
        date=date_value,
        fullness = data["fullness"],
        sales=Items(**data["sales"]),
        orders=Items(**data["orders"]),
    )
    await MarketplacesEcom.insert_one(marketplaces_obj)
    logger.info("Marketplaces: Данные успешно записаны!\n", f"'{marketplaces_obj}'")
    return marketplaces_obj

async def write_data():
    date_value = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)).date()
    data = await e_com_data(date_value)
    marketplaces_obj = MarketplacesEcom(
        date=date_value,
        fullness = data["fullness"],
        sales=Items(**data["sales"]),
        orders=Items(**data["orders"]),
    )

    await MarketplacesEcom.insert_one(marketplaces_obj)
    
    logger.info(f"Marketplaces: Данные успешно записаны!")
