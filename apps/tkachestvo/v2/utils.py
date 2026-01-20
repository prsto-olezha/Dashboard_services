from .models import Tkachestvo_model, Section_data, Field_data
import dateutil.parser
from core.logger import logger
import datetime
import core.backend as backend
# ================================


async def tkachestvo_stats_aggregation(
    date_from, date_to, skip, limit, year: int = None, month: int = None
):
    try:
        if year == None and month == None:
            year = datetime.datetime.now(datetime.timezone.utc).year
            month = datetime.datetime.now(datetime.timezone.utc).month
            match_conditions = {
                "$and": [
                    {"$eq": [{"$year": "$date"}, year]},
                    {"$eq": [{"$month": "$date"}, month]},
                ]
            }
        elif year != None and month == None:
            match_conditions = {
                "$and": [
                    {"$eq": [{"$year": "$date"}, year]},
                ]
            }
        elif year == None and month != None:
            match_conditions = {"$and": [{"$eq": [{"$month": "$date"}, month]}]}
        else:
            match_conditions = {
                "$and": [
                    {"$eq": [{"$year": "$date"}, year]},
                    {"$eq": [{"$month": "$date"}, month]},
                ]
            }
        pipeline = [
            {
                "$match": {
                    "date": {
                        "$gte": dateutil.parser.parse(f"{date_from}"),
                        "$lte": dateutil.parser.parse(f"{date_to}"),
                    },
                    # "$expr": match_conditions,
                }
            },
            {
                "$group": {
                    "_id": "$date",
                    "id": {"$last": "$_id"},
                    "created_at": {"$last": "$created_at"},
                    "date": {"$last": "$date"},
                    "tkachestvo": {"$last": "$tkachestvo"},
                    "weaving": {"$last": "$weaving"},
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
                    "tkachestvo": 1,
                    "weaving": 1,
                    "fullness": 1,
                    "fullness_date": 1,
                    "fullness_descriptions": 1,
                }
            },
            {"$facet": {"result": [{"$skip": skip}, {"$limit": limit}]}},
            {"$project": {"result": 1, "total": {"$size": "$result"}}},
        ]
        result = await Tkachestvo_model.aggregate(aggregation_pipeline=pipeline).to_list(
            length=None
        )
        return result[0]
    except Exception as ex:
        logger.error(ex)


async def sum_tkachestvo_stats_aggregation(date_from, date_to, year: int = None, month: int = None):
    try:
        if year is None:
            year = datetime.datetime.now(datetime.timezone.utc).year
        if month is None:
            month = datetime.datetime.now(datetime.timezone.utc).month

        match_conditions = {
            "$expr": {
                "$and": [
                    {"$eq": [{"$year": "$date"}, year]},
                    {"$eq": [{"$month": "$date"}, month]}
                ]
            }
        }

        pipeline = [
            {
                "$match": {
                    "created_at": {
                        "$gte": dateutil.parser.parse(f"{date_from}"),
                        "$lte": dateutil.parser.parse(f"{date_to}"),
                    },
                    "$expr": match_conditions,
                }
            },
            {
                "$group": {
                    "_id": "$date",
                    "created_at": {"$last": "$created_at"},
                    "fullness": {"$last": "$fullness"},
                    "fullness_date": {"$last": "$fullness_date"},
                    "fullness_descriptions": {"$last": "$fullness_descriptions"},
                    "date": {"$last": "$date"},
                    "f_ed": {"$last": "$f_ed"},
                    "percent_work": {"$last": "$percent_work"},
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
                    'fullness_date': [
                        {"$sort": {"created_at": 1}},
                        {   
                            '$group': {
                                '_id': 0, 
                                'fullness_date': {
                                    '$last': '$fullness_date'
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
                    "f_ed_sum": [
                        {"$group": {"_id": None, "value": {"$sum": "$f_ed"}}}
                    ],
                    "percent_work_avg": [
                        {"$group": {"_id": None, "value": {"$avg": "$percent_work"}}}
                    ],
                }
            },
            {
                "$project": {
                    "result": {
                        'fullness': {
                                '$arrayElemAt': ['$fullness.fullness', 0]
                            },
                        'fullness_date': {
                                '$arrayElemAt': ['$fullness_date.fullness_date', 0]
                            },
                        "fullness_descriptions": {
                            '$arrayElemAt': ['$fullness_descriptions.fullness_descriptions', 0]
                        },
                        "f_ed_sum": {
                            "$ifNull": [{"$arrayElemAt": ["$f_ed_sum.value", 0]}, 0]
                        },
                        "percent_work_avg": {
                            "$ifNull": [{"$arrayElemAt": ["$percent_work_avg.value", 0]}, 0]
                        },
                    }
                }
            }
        ]

        result = await Tkachestvo_model.aggregate(pipeline).to_list(length=None)
        return result[0] if result else {"f_ed_sum": 0, "percent_work_avg": 0}

    except Exception as ex:
        logger.error(f"Ошибка в агрегации: {ex}")
        return None


async def get_last_fullness_date_tkachestvo():
    try:
        pipeline = [
            # Фильтруем записи, где fullness_date не null
            {"$match": {"tkachestvo.fullness_date": {"$ne": None}, "tkachestvo.fullness": True}},
            # Сортируем по убыванию create_at (самая новая запись первой)
            {"$sort": {"created_at": -1}},
            # Оставляем только поле fullness_date
            {"$project": {"fullness_date": "$tkachestvo.fullness_date"}}
        ]
        
        result = await Tkachestvo_model.aggregate(pipeline).to_list(length=10)
        print(result[0]["fullness_date"].date())
        return result[0]["fullness_date"].date() if result else None  # Возвращаем только fullness_date
    except Exception as ex:
        logger.error(f"Ошибка в get_last_fullness_date: {ex}")
        return None

async def get_last_fullness_date_weaving():
    try:
        pipeline = [
            # Фильтруем записи, где fullness_date не null
            {"$match": {"weaving.fullness_date": {"$ne": None}, "weaving.fullness": True}},
            # Сортируем по убыванию create_at (самая новая запись первой)
            {"$sort": {"created_at": -1}},
            # Оставляем только поле fullness_date
            {"$project": {"fullness_date": "$tkachestvo.fullness_date"}}
        ]
        
        result = await Tkachestvo_model.aggregate(pipeline).to_list(length=10)
        print(result[0]["fullness_date"].date())
        return result[0]["fullness_date"].date() if result else None  # Возвращаем только fullness_date
    except Exception as ex:
        logger.error(f"Ошибка в get_last_fullness_date: {ex}")
        return None

async def manual_write_data(date_value: datetime.date, data):
    print([data["day"][section] for section in data["day"]])
    tkachestvo_obj = Tkachestvo_model(
        date=date_value,
        day = [data["day"][section] for section in data["day"]],
        until_day = [data["until_day"][section] for section in data["until_day"]],
        fullness = data["fullness"],
        fullness_date = data["fullness_date"],
        fullness_descriptions = data["fullness_descriptions"],
    )
    data_obj = await Tkachestvo_model.insert_one(tkachestvo_obj)
    logger.info("Tkachestvo: Данные успешно записаны!")
    return data_obj


async def write_data():
    for i in range(4, 0, -1):
        date_value = (
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=i)
        ).date()
        
        data = await backend.tkachestvo_data(date_value)
        print([data["day"][section] for section in data["day"]])
        tkachestvo_obj = Tkachestvo_model(
            date=date_value,
            day = [data["day"][section] for section in data["day"]],
            until_day = [data["until_day"][section] for section in data["until_day"]],
            fullness = data["fullness"],
            fullness_date = data["fullness_date"],
            fullness_descriptions = data["fullness_descriptions"],
        )
        await Tkachestvo_model.insert_one(tkachestvo_obj)
        logger.info(f"Tkachestvo: Данные за {date_value} успешно записаны!")
