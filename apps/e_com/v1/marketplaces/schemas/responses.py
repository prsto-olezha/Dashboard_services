from ..models import MarketplacesEcom
from pydantic import BaseModel


class MarketplacesGetStatsResponse(BaseModel):
	result: list[MarketplacesEcom, None]
	total: int | None = None

