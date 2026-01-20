from ..models import EcomDeviation_model
from pydantic import BaseModel


class EcomDeviationGetStatsResponse(BaseModel):
	result: list[EcomDeviation_model, None]
	total: int | None = None

