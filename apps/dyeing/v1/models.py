from beanie import Document
from pydantic import BaseModel, Field
import datetime as dt


class ItemData(BaseModel):
    plan: float | None = None
    plan_total: float | None = None
    oper: float | None = None
    oper_total: float | None = None
    fact: float | None = None
    fact_total: float | None = None
    unit: str | None = None

class Dyeing_model(Document):
    created_at: dt.datetime = Field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))
    date: dt.datetime | None = None
    fullness: bool | None = None
    saver: ItemData
    dmc1: ItemData
    dmc2: ItemData
    kusters1: ItemData
    kusters2: ItemData
    henriksen1: ItemData
    henriksen2: ItemData
    total: ItemData

    class Settings:
        name = "Dyeing"


