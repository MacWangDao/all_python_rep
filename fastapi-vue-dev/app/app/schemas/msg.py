from typing import Optional, Any

from pydantic import BaseModel
from pydantic.networks import EmailStr


class Msg(BaseModel):
    code: Optional[int] = None
    data: Optional[Any] = None
    message: Optional[str] = None


class Email(BaseModel):
    email: EmailStr
