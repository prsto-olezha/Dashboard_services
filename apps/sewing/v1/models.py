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
    
class product(BaseModel):
    clothes: ItemData
    kitchen: ItemData
    bedroom: ItemData
    shirts: ItemData
    total: ItemData
    
class production(BaseModel):
    inside: product
    outside: product
    total: ItemData
    
class Sewing_model(Document):
    created_at: dt.datetime = Field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))
    date: dt.datetime | None = None
    fullness: bool | None = None
    kpb: production
    rub: production

    class Settings:
        name = "Sewing"


