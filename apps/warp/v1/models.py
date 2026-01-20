from beanie import Document
from pydantic import BaseModel, Field
import datetime as dt
from typing import Dict

class ItemData(BaseModel):
    count: int | None = None
    manufacturer: str | None = None
    lot: int | str | None = None
    meters: float | None = None
    break_count: int | None = None
    breakage_rate: float | None = None
    
class Warp_model(Document):
    created_at: dt.datetime = Field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))
    date: dt.datetime | None = None
    fullness: bool | None = None
    cotton: Dict[str, ItemData]= Field(default_factory=dict)
    aramid: Dict[str, ItemData]= Field(default_factory=dict)
    linen: Dict[str, ItemData]= Field(default_factory=dict)

    class Settings:
        name = "Warp"


