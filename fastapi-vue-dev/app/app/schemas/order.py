import datetime
from typing import Optional, List

from pydantic import BaseModel


class StraNameBase(BaseModel):
    straname: Optional[str]
    sid: Optional[int] = None
    status: int = 1

    class Config:
        orm_mode = True


class StraBase(BaseModel):
    straname: Optional[str]
    type: int = 0
    period: int = None
    step: int = None
    cancel: int = None
    limit: int = 0
    ctime: datetime.datetime = datetime.datetime.now()
    status: int = 1


# Properties to receive via API on creation
class StraCreate(StraBase):
    ...


# Properties to receive via API on update
class StraUpdate(StraBase):
    sid: Optional[int] = None

    class Config:
        orm_mode = True


class StraDel(StraBase):
    sid: Optional[int] = None


class StraRemove(StraBase):
    sid: Optional[int] = None


class StraInDBBase(StraBase):
    sid: Optional[int] = None

    class Config:
        orm_mode = True


class Stra(StraInDBBase):
    ...
