from .models import Line_model
import dateutil.parser
from core.logger import logger
import datetime
import core.backend as backend
# ================================

async def manual_write_data(date_value: datetime.date, data):
    line_obj_array = []
    for line in data:
        line_obj = Line_model(
            index = line["index"],
            data = line["data"]
        )
        line_obj_array.append(line_obj)
        
    data_obj = await Line_model.insert_many(line_obj_array)
    logger.info("Tkachestvo: Данные успешно записаны!")
    return data_obj


# async def write_data():
#     for i in range(4, 0, -1):
#         date_value = (
#             datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=i)
#         ).date()
        
#         data = await backend.tkachestvo_data(date_value)
#         print([data["day"][section] for section in data["day"]])
#         tkachestvo_obj = Tkachestvo_model(
#             date=date_value,
#             day = [data["day"][section] for section in data["day"]],
#             until_day = [data["until_day"][section] for section in data["until_day"]],
#             fullness = data["fullness"],
#             fullness_date = data["fullness_date"],
#             fullness_descriptions = data["fullness_descriptions"],
#         )
#         await Tkachestvo_model.insert_one(tkachestvo_obj)
#         logger.info(f"Tkachestvo: Данные за {date_value} успешно записаны!")
