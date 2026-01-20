from ..models import Sales_plan_model
from pydantic import BaseModel


class SalesPlanGetStatsResponse(BaseModel):
	result: list[Sales_plan_model, None]
	total: int | None = None

