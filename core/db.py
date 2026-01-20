from motor.motor_asyncio import AsyncIOMotorClient
from beanie import init_beanie
from core.config import MONGO_DB_CONNECT

# mongo
MONGO_DB_URL = f"mongodb://{MONGO_DB_CONNECT['username']}:{MONGO_DB_CONNECT['password']}@{MONGO_DB_CONNECT['host']}:{MONGO_DB_CONNECT['port']}/"

async def init_mongo_db(models_services, models_backup):
    client = AsyncIOMotorClient(MONGO_DB_URL, uuidRepresentation="standard")
    await init_beanie(client[MONGO_DB_CONNECT['db_services']], document_models=models_services)
    await init_beanie(client[MONGO_DB_CONNECT['db_backup']], document_models=models_backup)
