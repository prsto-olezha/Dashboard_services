import os
from dotenv import load_dotenv
#===============
load_dotenv()

APP_HOST = os.getenv('APP_HOST')
APP_PORT = int(os.getenv('APP_PORT'))

MONGO_DB_CONNECT = {
    'db_services': os.getenv("MONGODB_DB_SERVICES"),
    'db_backup': os.getenv("MONGODB_DB_BACKUP"),
    'host': os.getenv("MONGODB_HOST"),
    'port': int(os.getenv("MONGODB_PORT")),
    'username': os.getenv("MONGODB_USERNAME"),
    'password': os.getenv("MONGODB_PASSWORD"),
}

POSTGRE_DB_CONNTECT = {
    'db': os.getenv("POSTGRE_DB"),
    'host': os.getenv("POSTGRE_HOST"),
    'port': int(os.getenv("POSTGRE_PORT")),
    'username': os.getenv("POSTGRE_USERNAME"),
    'password': os.getenv("POSTGRE_PASSWORD"),
}

month_dict = {1: "Январь",
                  2: "Февраль", 
                  3: "Март",
                  4: "Апрель",
                  5: "Май",
                  6: "Июнь",
                  7: "Июль",
                  8: "Август",
                  9: "Сентябрь",
                  10: "Октябрь",
                  11: "Ноябрь",
                  12: "Декабрь"
                  }


month_dict_rev = {"январь": 1,
                  "февраль": 2, 
                  "март": 3,
                  "апрель": 4,
                  "май": 5,
                  "июнь": 6,
                  "июль": 7,
                  "август": 8,
                  "сентябрь": 9,
                  "октябрь": 10,
                  "ноябрь": 11,
                  "декабрь": 12
                  }