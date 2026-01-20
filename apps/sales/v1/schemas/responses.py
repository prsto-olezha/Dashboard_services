from ..models import Sales_model
from pydantic import BaseModel


class SalesGetStatsResponse(BaseModel):
	result: list[Sales_model, None]
	total: int | None = None
