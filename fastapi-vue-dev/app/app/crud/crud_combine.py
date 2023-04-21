from typing import Any, Dict, Union

from sqlalchemy.orm import Session

from app.crud.base import CRUDBase
from app.models.strategy_stock import Strategy_Stock_Info
from app.schemas.combine import CombinenameCreate, CombineUpdate


class CRUDCombine(CRUDBase[Strategy_Stock_Info, CombinenameCreate, CombineUpdate]):

    def create(self, db: Session, *, obj_in: CombinenameCreate) -> Strategy_Stock_Info:
        create_data = obj_in.dict()
        db_obj = Strategy_Stock_Info(**create_data)
        db.add(db_obj)
        db.commit()
        db.flush()
        db.refresh(db_obj)
        return db_obj

    # def update(
    #         self, db: Session, *, db_obj: Strategy_Stock_Info, obj_in: Union[StraUpdate, Dict[str, Any]]
    # ) -> Strategy:
    #     if isinstance(obj_in, dict):
    #         update_data = obj_in
    #     else:
    #         update_data = obj_in.dict(exclude_unset=True)
    #     db.query(Strategy).filter(Strategy.sid == db_obj.sid).update(update_data)
    #     db.commit()
    #     db.flush()
    #     return db_obj
    #
    # def remove(self, db: Session, *, sid: int) -> None:
    #     super().remove(db, sid=sid)


combine = CRUDCombine(Strategy_Stock_Info)
