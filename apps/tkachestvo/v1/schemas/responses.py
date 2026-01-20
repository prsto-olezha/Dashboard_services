from ..models import Tkachestvo_model
from pydantic import BaseModel

class TkachestvoGetStatsResponse(BaseModel):
	result: list[Tkachestvo_model, None]
	total: int | None = None