from ..models import Sewing_model
from pydantic import BaseModel


class SewingGetStatsResponse(BaseModel):
	result: list[Sewing_model, None]
	total: int | None = None

