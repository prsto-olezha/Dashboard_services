from ..models import Weaving_model
from pydantic import BaseModel


class WeavingGetStatsResponse(BaseModel):
	result: list[Weaving_model, None]
	total: int | None = None

