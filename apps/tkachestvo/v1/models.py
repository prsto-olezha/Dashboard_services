from beanie import Document
from pydantic import BaseModel, Field
from typing import List
import datetime as dt

class DayData_Tkachestvo(BaseModel):
    f_ed: float | None = None
    percent_work: float | None = None

class UntilDayData_Tkachestvo(BaseModel):
    f_ed: float | None = None
    percent_work: float | None = None

class TkachestvoData(BaseModel):
    day: DayData_Tkachestvo
    until_day: UntilDayData_Tkachestvo
    fullness: bool | None = None
    fullness_date: dt.datetime | None = None

class DayData_Weaving(BaseModel):
    percent_plan: float | None = None

class UntilDayData_Weaving(BaseModel):
    percent_plan: float | None = None
    
class WeavingData(BaseModel):
    day: DayData_Weaving
    until_day: UntilDayData_Weaving
    fullness: bool | None = None
    fullness_date: dt.datetime | None = None
    
class Tkachestvo_model(Document):
    created_at: dt.datetime | None = Field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))
    date: dt.datetime | None = None
    tkachestvo: TkachestvoData
    weaving: WeavingData
    fullness: bool | None = None
    fullness_date: dt.datetime | None = None
    fullness_descriptions: str | None = None
    class Settings:
        name = "Tkachestvo"