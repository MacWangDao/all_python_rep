import os

from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from app.db.base_class import Base


class Role(Base):
    """权限组"""
    rid = Column(Integer, primary_key=True, index=True, autoincrement=True)
    rolename = Column(String(32), doc="权限组名称")
    description = Column(String(128), doc="备注")
    order = Column(Integer, doc="顺序")

    role_menu = relationship("Role_Menu", backref="role")

    def to_dict(self):
        return {c.name: getattr(self, c.name, None) for c in self.__table__.columns}

    def __repr__(self):
        return f"ssid:{self.ssid},order_id:{self.order_id},orderno:{self.orderno}" \
               f",sessionid:{self.sessionid},status:{self.status},cdatetime:{self.cdatetime}"


class Role_Menu(Base):
    """权限组-菜单-中间表"""
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    role_id = Column(Integer, ForeignKey("role.id", ondelete='CASCADE'))
    menu_id = Column(Integer, ForeignKey("menu.id", ondelete='CASCADE'))

    def to_dict(self):
        return {c.name: getattr(self, c.name, None) for c in self.__table__.columns}

    def __repr__(self):
        return f"ssid:{self.ssid},order_id:{self.order_id},orderno:{self.orderno}" \
               f",sessionid:{self.sessionid},status:{self.status},cdatetime:{self.cdatetime}"


if __name__ == "__main__":
    import cx_Oracle
    from sqlalchemy import create_engine

    os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
    dns = cx_Oracle.makedsn('192.168.101.215', 1521, service_name='bjhy')
    engine = create_engine("oracle://bjhy:bjhy@" + dns, encoding='utf-8', echo=True)
    # Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
