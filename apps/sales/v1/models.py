from beanie import Document
from pydantic import BaseModel, Field
import datetime as dt
from typing import List, Dict, Annotated

class SalesColumn(BaseModel):
    plan: float | None = None
    oper: float | None = None
    fact: float | None = None
    deviation: float | None = None

class CashFlowColumn(BaseModel):
    fullness: bool | None = None
    fullness_date: dt.datetime | None = None
    plan: float | None = None
    oper: float | None = None
    fact: float | None = None
    deviation: float | None = None

class Sales_model(Document):
    created_at: dt.datetime = Field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))
    date: dt.datetime | None
    fullness: bool | None = None
    fullness_date: dt.datetime | None = None
    fullness_descriptions: str | None = None
    sales: SalesColumn
    cash_flow: CashFlowColumn
    class Settings:
        name = "Sales"



