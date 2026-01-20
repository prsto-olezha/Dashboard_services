from beanie import Document
from pydantic import BaseModel, Field
import datetime as dt


class ItemData(BaseModel):
    plan: float | None = None
    operSorting: float | None = None
    operFinishing: float | None = None
    factSorting: float | None = None
    factFinishing: float | None = None
    proc_planFinishing: float | None = None


class Finishing_model(Document):
    created_at: dt.datetime = Field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))
    fullness: bool | None = None
    dyed: ItemData | None = None
    bleached: ItemData | None = None
    multicolored: ItemData | None = None
    stamp: ItemData | None = None
    washed: ItemData | None = None
    raw: ItemData | None = None
    total: ItemData | None = None

    class Settings:
        name = "Finishing"


