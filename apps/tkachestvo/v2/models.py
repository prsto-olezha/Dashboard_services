from beanie import Document
from pydantic import BaseModel, Field
from typing import List, Dict
import datetime as dt

class Field_data(BaseModel):
    value: float | None = None
    value_total: float | None = None
    unit: str | None = None

class Section_data(BaseModel):
    section_name: str
    f_ed: Field_data
    percent_fact: Field_data
    percent_oper: Field_data
    output_pm: Field_data
    output_mil_m_ut: Field_data

class Tkachestvo_model(Document):
    created_at: dt.datetime | None = Field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))
    date: dt.datetime | None = None
    day: List[Section_data]
    until_day: List[Section_data]
    fullness: bool | None = None
    fullness_date: dt.datetime | None = None
    fullness_descriptions: str | None = None
    class Settings:
        name = "Tkachestvo_v2"
    