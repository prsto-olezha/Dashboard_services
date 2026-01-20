from beanie import Document
from pydantic import BaseModel, Field
import datetime as dt

class ItemData(BaseModel):
    plan: float | None = None
    oper: float | None = None
    plan_total: float | None = None
    oper_total: float | None = None
    deviation: float |  None = None
    unit: str | None = None

class DayData(BaseModel):
    redemption_rate: float | None = None  # Коэффициент выкупа
    profitability: float | None = None   # Рентабельность
    profitability_ecom: float | None = None   # Рентабельность
    localization_rate: float | None = None # Коэффициент локализации
    turnover_ratio: float | None = None    # Коэффициент оборачиваемости
    ecom_share: float | None = None       # Доля E-com от продаж
    
class Days7Data(BaseModel):
    redemption_rate: float | None = None  # Коэффициент выкупа
    profitability: float | None = None   # Рентабельность
    profitability_ecom: float | None = None   # Рентабельность
    localization_rate: float | None = None # Коэффициент локализации
    turnover_ratio: float | None = None    # Коэффициент оборачиваемости
    ecom_share: float | None = None       # Доля E-com от продаж

class Days30Data(BaseModel):
    redemption_rate: float | None = None  # Коэффициент выкупа
    profitability: float | None = None   # Рентабельность
    profitability_ecom: float | None = None   # Рентабельность
    localization_rate: float | None = None # Коэффициент локализации
    turnover_ratio: float | None = None    # Коэффициент оборачиваемости
    ecom_share: float | None = None       # Доля E-com от продаж

class TableData(BaseModel):
    day: DayData
    days7: Days7Data
    days30: Days30Data

class EcomDeviation_model(Document):
    created_at: dt.datetime | None = Field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))
    date: dt.datetime | None = None
    sales: ItemData
    orders: ItemData
    table: TableData
    fullness: bool | None = None
    fullness_date: dt.datetime | None = None
    fullness_descriptions: str | None = None
    fullness: bool | None = None



