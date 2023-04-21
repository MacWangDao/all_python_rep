from typing import Optional, Any

from pydantic import BaseModel


# Shared properties
class Twap_PovBase(BaseModel):
    stockcode: str
    price: float
    volume: int
    period: int
    step: int
    cancel: int
    exchangeid: int
    bsdir: int
    limit: int
    type: int


# Properties to receive on item creation
class Twap_PovCreate(Twap_PovBase):
    pass


# Properties to receive on item update
class Twap_PovUpdate(Twap_PovBase):
    pass
