from ..models import Dyeing_model
from pydantic import BaseModel


class DyeingGetStatsResponse(BaseModel):
	result: list[Dyeing_model, None]
	total: int | None = None

