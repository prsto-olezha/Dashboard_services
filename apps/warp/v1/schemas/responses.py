from ..models import Warp_model
from pydantic import BaseModel


class WarpGetStatsResponse(BaseModel):
	result: list[Warp_model, None]
	total: int | None = None

