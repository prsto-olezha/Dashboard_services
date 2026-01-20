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
    proc_plan: float | None = None 

class Weaving_model(Document):
	created_at: dt.datetime | None = Field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))
	date: dt.datetime | None = None
	fullness: bool | None = None
	pnevmat: ItemData
	rapira: ItemData
	total: ItemData

	class Settings:
		name = "Weaving"


