from beanie import Document
from pydantic import BaseModel, Field
from typing import List, Dict
import datetime as dt

class Line_model(Document):
    created_at: dt.datetime | None = Field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))
    index: int
    data: Dict
    class Settings:
        name = "Tkachestvo_v3"
    