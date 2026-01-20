from ..models import Finishing_model
from pydantic import BaseModel


class FinishingGetStatsResponse(BaseModel):
	result: list[Finishing_model, None]
	total: int | None = None

