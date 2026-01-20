from ..models import Line_model
from pydantic import BaseModel


class TkachestvoGetStatsResponse(BaseModel):
	result: list[Line_model, None]
	total: int | None = None

