from ..models import Yarn_model
from pydantic import BaseModel


class YarnGetStatsResponse(BaseModel):
	result: list[Yarn_model, None]
	total: int | None = None

