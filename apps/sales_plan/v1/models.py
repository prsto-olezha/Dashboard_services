from beanie import Document
from pydantic import BaseModel, Field
import datetime as dt
from typing import List, Dict, Annotated

class ManagerData(BaseModel):
    manager: str | None = None
    plan: float | None = None
    oper: float | None = None
    fact: float | None = None

class ItemData(BaseModel):
    plan: float | None = None
    oper: float | None = None
    fact: float | None = None

class Item(BaseModel):
    managers: List[ManagerData]
    total: ItemData

class Section(BaseModel):
    total: Item
    fabrics: Item
    goods: Item
    e_com: Item
    manager1: Item
    manager2: Item
    manager3: Item
    
class Sales_plan_model(Document):
    created_at: dt.datetime = Field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))
    shipments: Section
    receipts: Section
    fullness: bool | None = None

    class Settings:
        name = "Sales_plan"


