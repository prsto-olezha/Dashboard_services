from .models import Yarn_model, Yarn_model_backup
import dateutil.parser
from core.logger import logger
import datetime
from .models import ItemData, ItemData_Backup, TotalData
import core.backend as backend

# ================================


async def yarn_stats_aggregation(date_from, date_to, skip, limit, year:int = None, month:int = None):
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
                    "fullness_date": {"$last": "$fullness_date"},
                    "fullness_descriptions": {"$last": "$fullness_descriptions"},
                    "field_68": {"$last": "$field_68"},
                    "field_20": {"$last": "$field_20"},
                    "field_54_2": {"$last": "$field_54_2"},
                    "field_40_2": {"$last": "$field_40_2"},
                    "field_25_smes": {"$last": "$field_25_smes"},
                    "total": {"$last": "$total"}
                }
            },
            {
                "$project": {
                    "_id": "$id",
                    "created_at": 1,
                    "date": 1,
                    "fullness": 1,
                    "fullness_date": 1,
                    "fullness_descriptions": 1,
                    "field_68": 1,
                    "field_20": 1,
                    "field_54_2": 1,
                    "field_40_2": 1,
                    "field_25_smes": 1,
                    "total": 1
                }
            },
            {"$facet": {"result": [{"$skip": skip}, {"$limit": limit}]}},
            {"$project": {"result": 1, "total": {"$size": "$result"}}},
        ]
        result = await Yarn_model.aggregate(aggregation_pipeline=pipeline).to_list(
            length=None
        )
        return result[0]
    except Exception as ex:
        logger.error(ex)


async def sum_yarn_stats_aggregation(date_from, date_to, year:int = None, month:int = None):
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
                    "fullness_descriptions": {"$last": "$fullness_descriptions"}, 
                    "field_68": {"$last": "$field_68"},
                    "field_20": {"$last": "$field_20"},
                    "field_54_2": {"$last": "$field_54_2"},
                    "field_40_2": {"$last": "$field_40_2"},
                    "field_25_smes": {"$last": "$field_25_smes"},
                    "total": {"$last": "$total"}
                }
            },
            {
                "$facet":
                    {'fullness': [
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
                    'fullness_descriptions': [
                        {"$sort": {"created_at": 1}},
                        {   
                            '$group': {
                                '_id': 0, 
                                'fullness_descriptions': {
                                    '$last': '$fullness_descriptions'
                                }
                            }
                        }
                    ],
                    "field_68_sum": [
                        {
                            "$group": {
                                "_id": 0,
                                "remainder": {"$sum": "$field_68.remainder_total"},
                                "consumption": {"$sum": "$field_68.consumption_total"},
                            }
                        }
                    ],
                    "field_20_sum": [
                        {
                            "$group": {
                                "_id": 0,
                                "remainder": {"$sum": "$field_20.remainder_total"},
                                "consumption": {"$sum": "$field_20.consumption_total"},
                            }
                        }
                    ],
                    "field_54_2_sum": [
                        {
                            "$group": {
                                "_id": 0,
                                "remainder": {"$sum": "$field_54_2.remainder_total"},
                                "consumption": {"$sum": "$field_54_2.consumption_total"},
                            }
                        }
                    ],
                    "field_40_2_sum": [
                        {
                            "$group": {
                                "_id": 0,
                                "remainder": {"$sum": "$field_40_2.remainder_total"},
                                "consumption": {"$sum": "$field_40_2.consumption_total"},
                            }
                        }
                    ],
                    "field_25_smes_sum": [
                        {
                            "$group": {
                                "_id": 0,
                                "remainder": {"$sum": "$field_25_smes.remainder_total"},
                                "consumption": {"$sum": "$field_25_smes.consumption_total"},
                            }
                        }
                    ],
                    "total_sum": [
                        {
                            "$group": {
                                "_id": 0,
                                "remainder": {"$sum": "$total.remainder_total"},
                                "consumption": {"$sum": "$total.consumption_total"},
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
                            "fullness_descriptions": {
                                '$arrayElemAt': ['$fullness_descriptions.fullness_descriptions', 0]
                            },
                            "field_68": {
                                "remainder": {"$arrayElemAt": ["$field_68_sum.remainder", 0]},
                                "consumption": {"$arrayElemAt": ["$field_68_sum.consumption", 0]},
                            },
                            "field_20": {
                                "remainder": {"$arrayElemAt": ["$field_20_sum.remainder", 0]},
                                "consumption": {"$arrayElemAt": ["$field_20_sum.consumption", 0]},
                            },
                            "field_54_2": {
                                "remainder": {"$arrayElemAt": ["$field_54_2_sum.remainder", 0]},
                                "consumption": {"$arrayElemAt": ["$field_54_2_sum.consumption", 0]},
                            },
                            "field_40_2": {
                                "remainder": {"$arrayElemAt": ["$field_40_2_sum.remainder", 0]},
                                "consumption": {"$arrayElemAt": ["$field_40_2_sum.consumption", 0]},
                            },
                            "field_25_smes": {
                                "remainder": {"$arrayElemAt": ["$field_25_smes_sum.remainder", 0]},
                                "consumption": {"$arrayElemAt": ["$field_25_smes_sum.consumption", 0]},
                            },
                            "total": {
                                "remainder": {"$arrayElemAt": ["$total_sum.remainder", 0]},
                                "consumption": {"$arrayElemAt": ["$total_sum.consumption", 0]},
                            },
                            
                        }
                    ]
                }
            },
        ]
        result = await Yarn_model.aggregate(aggregation_pipeline=pipeline).to_list(
            length=None
        )
        return result[0]
    except Exception as ex:
        logger.error(ex)

async def yarn_screen_stats_aggregation(date_from, date_to, limit, year:int = None, month:int = None):
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
                    "fullness_date": {"$last": "$fullness_date"},
                    "fullness_descriptions": {"$last": "$fullness_descriptions"},
                    "field_68": {"$last": "$field_68"},
                    "field_20": {"$last": "$field_20"},
                    "field_54_2": {"$last": "$field_54_2"},
                    "field_40_2": {"$last": "$field_40_2"},
                    "field_25_smes": {"$last": "$field_25_smes"},
                    "total": {"$last": "$total"}
                }
            },
            {
                "$project": {
                    "_id": "$id",
                    "created_at": 1,
                    "date": 1,
                    "fullness": 1,
                    "fullness_date": 1,
                    "fullness_descriptions": 1,
                    "field_68": 1,
                    "field_20": 1,
                    "field_54_2": 1,
                    "field_40_2": 1,
                    "field_25_smes": 1,
                    "total": 1
                }
            },
            {"$facet": {"result": [{"$skip": skip}, {"$limit": limit}]}},
            {"$project": {"result": 1, "total": {"$size": "$result"}}},
        ]
        result = await Yarn_model.aggregate(aggregation_pipeline=pipeline).to_list(
            length=None
        )
        return result[0]
    except Exception as ex:
        logger.error(ex)
        
        
async def get_last_fullness_date():
    try:
        pipeline = [
            # Фильтруем записи, где fullness_date не null
            {"$match": {"fullness_date": {"$ne": None}, "fullness": True}},
            # Сортируем по убыванию create_at (самая новая запись первой)
            {"$sort": {"created_at": -1}},
            # Оставляем только поле fullness_date
            {"$project": {"fullness_date": 1}}
        ]
        
        result = await Yarn_model.aggregate(pipeline).to_list(length=10)
        return result[0]["fullness_date"].date() if result else None  # Возвращаем только fullness_date
    
    except Exception as ex:
        logger.error(f"Ошибка в get_last_fullness_date: {ex}")
        return None



async def manual_write_data(date_value: datetime.date, data):
    yarn_obj = Yarn_model(
        date=date_value,
        fullness = data["fullness"],
        fullness_date = data["fullness_date"],
        fullness_descriptions = data["fullness_descriptions"],
        field_68 = ItemData(**data["field_68"]),
        field_20 = ItemData(**data["field_20"]),
        field_54_2 = ItemData(**data["field_54_2"]),
        field_40_2 = ItemData(**data["field_40_2"]),
        field_25_smes = ItemData(**data["field_25_smes"]),
        total = TotalData(**data["total"]),
    )
    await Yarn_model.insert_one(yarn_obj)
    logger.info("Yarn: Данные успешно записаны!")
    return yarn_obj

async def write_data():
    for i in range(-1, 30):
        date_value = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=i)).date()
        data = await backend.yarn_data(date_value)
        yarn_obj = Yarn_model(
            date=date_value,
            fullness = data["fullness"],
            fullness_date = data["fullness_date"],
            fullness_descriptions = data["fullness_descriptions"],
            field_68 = ItemData(**data["field_68"]),
            field_20 = ItemData(**data["field_20"]),
            field_54_2 = ItemData(**data["field_54_2"]),
            field_40_2 = ItemData(**data["field_40_2"]),
            field_25_smes = ItemData(**data["field_25_smes"]),
            total=TotalData(**data["total"]),
        )
        await Yarn_model.insert_one(yarn_obj)
        logger.info(f"Yarn: Данные за {date_value} успешно записаны!")

async def backup_data():
    date_value = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=1)).date()
    data = await backend.yarn_data_backup(date_value)
    yarn_obj = Yarn_model_backup(
        date=date_value,
        field_68 = ItemData_Backup(**data["field_68"]),
        field_20 = ItemData_Backup(**data["field_20"]),
        field_54_2 = ItemData_Backup(**data["field_54_2"]),
        field_40_2 = ItemData_Backup(**data["field_40_2"]),
        field_25_smes = ItemData_Backup(**data["field_25_smes"]),
    )
    await Yarn_model_backup.insert_one(yarn_obj)
    logger.info(f"Yarn_Backup: Данные за {date_value} успешно записаны!")
    
async def get_data_backup(date: datetime.datetime):
    backup_obj = await Yarn_model_backup.find(Yarn_model_backup.date == date).to_list()
    if backup_obj:
        return backup_obj[0].model_dump()  # Преобразует Pydantic-модель в словарь
    return None