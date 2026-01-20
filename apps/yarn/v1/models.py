from beanie import Document
from pydantic import BaseModel, Field
import datetime as dt

class ItemData(BaseModel):
    remainder: float | None = None
    consumption: float | None = None
    remainder_total: float | None = None
    consumption_total: float | None = None
    KTZ: float | None = None
    KTZ_status: int | None = None
    payment: dt.date | None = None
    admission: dt.date | None = None

class TotalData(BaseModel):
    remainder: float | None = None
    consumption: float | None = None
    arrival: float | None = None
    remainder_total: float | None = None
    consumption_total: float | None = None
    arrival_total: float | None = None
    KTZ: float | None = None
    KTZ_status: int | None = None

class Yarn_model(Document):
    created_at: dt.datetime = Field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))
    date: dt.datetime | None = None
    fullness: bool | None = None
    fullness_date: dt.datetime | None = None
    fullness_descriptions: str | None = None
    field_68: ItemData
    field_20: ItemData
    field_54_2: ItemData
    field_40_2: ItemData
    field_25_smes: ItemData
    total: TotalData
    
    class Settings:
        name = "Yarn"


class ItemData_Backup(BaseModel):
    consumption_total: float | None = None
    
class Yarn_model_backup(Document):
    created_at: dt.datetime = Field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))
    date: dt.datetime | None = None
    field_68: ItemData_Backup
    field_20: ItemData_Backup
    field_54_2: ItemData_Backup
    field_40_2: ItemData_Backup
    field_25_smes: ItemData_Backup
    class Settings:
        name = "Yarn_backup"