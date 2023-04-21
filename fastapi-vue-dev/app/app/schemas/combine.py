import datetime
from typing import Optional, List

from pydantic import BaseModel


class CombineBase(BaseModel):
    ssname: Optional[str]


class CombinenameCreate(CombineBase):
    status: int = 1
    userid: Optional[int] = None
    ctime: datetime.datetime = datetime.datetime.now()


class CombinenameUpdate(CombineBase):
    ssi_id: Optional[int] = None

    class Config:
        orm_mode = True


class CombineTableLock(BaseModel):
    ssi_id: Optional[int] = None
    buylock: Optional[bool] = False
    selllock: Optional[bool] = False


class CombineinfoCreate(CombineBase):
    stockcode: Optional[str]
    exchangeid: int = 0
    price: float = 0.0
    vol: int = 0
    bsdir: int = 0
    ctime: datetime.datetime = datetime.datetime.now()
    status: int = 1
    straname: Optional[str]
    sid: Optional[int] = None
    ssiid: Optional[int] = None


class CombineUpdate(CombinenameCreate):
    ssname: Optional[str]
    straname: Optional[str]
    type: int = 0
    period: int = 0
    step: int = 0
    cancel: int = 0
    limit: int = 0
    stockcode: Optional[str]
    stockname: Optional[str]
    price: float = 0.0
    vol: int = 0
    bsdir: int = 0
    exchangeid: int = 0
    ssid: Optional[int] = None
    str_id: Optional[int] = None
    sto_id: Optional[int] = None
    ssi_id: Optional[int] = None

    class Config:
        orm_mode = True


class CombineDel(CombineBase):
    ssid: Optional[int] = None
    sto_id: Optional[int] = None
    ssi_id: Optional[int] = None


class CombineRemove(CombineBase):
    ssid: Optional[int] = None
    sto_id: Optional[int] = None
    ssi_id: Optional[int] = None


class CombineRemoveList(CombineBase):
    class OrderID(BaseModel):
        sto_id: Optional[int] = None
        ssi_id: Optional[int] = None

    ssid: Optional[int] = None
    order_list: List[OrderID] = None


class Order(BaseModel):
    ssid: Optional[int] = None
    str_id: Optional[int] = None
    sto_id: Optional[int] = None
    ssi_id: Optional[int] = None
    status: int = 1


class OrderList(BaseModel):
    # userid: Optional[int] = None
    account: Optional[str] = None
    order_list: List[Order] = None


class Cancel(BaseModel):
    securityid: Optional[str] = None
    orderno: Optional[str] = None
    sessionid: Optional[int] = 0


class CancelOrderList(BaseModel):
    ctype: Optional[bool] = True
    account: Optional[str] = None
    cancel_list: List[Cancel] = None


class Orders(BaseModel):
    ssi_id: Optional[int] = None
    account: Optional[str] = None


class UserList(BaseModel):
    userid: Optional[int] = None
    uname: Optional[str] = None
    usernum: Optional[str] = None
    account: Optional[str] = None
    password: Optional[str] = None
    status: Optional[int] = 0
    accdesc: Optional[str] = None


class AdminUpdate(UserList):
    acid: Optional[int] = None

    class Config:
        orm_mode = True


class AdminDel(BaseModel):
    acid: Optional[int] = None


class UserOrders(BaseModel):
    account: Optional[str] = None
    qtype: Optional[int] = None
    securityid: Optional[str] = None
    orderno: Optional[str] = None
    sessionid: Optional[int] = None


class CombineInDBBase(CombineBase):
    ssiid: Optional[int] = None

    class Config:
        orm_mode = True


class Combine(CombineInDBBase):
    ...
