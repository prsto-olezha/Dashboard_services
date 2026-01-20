async def fullness_info(date, file_name):
    return f"Файл '{file_name}' последний раз был заполнен {date}!"


async def NOFILE_ERROR(file_name):
    return f"Файл '{file_name}' отсутствует!"
