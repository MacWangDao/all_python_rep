import os

from sqlalchemy import Integer, String, Column, Boolean, Sequence
from sqlalchemy import Integer, String, Column, Sequence, Float, DateTime, ForeignKey, Table, Boolean
from sqlalchemy.orm import relationship, backref
from sqlalchemy.schema import Identity
from sqlalchemy.sql import func

from app.db.base_class import Base


# permission_role = Table(  # 定义中间表
#     'T_W_BJHY_ROLE_PERMISSION',  # 建立表的名字
#     Base.metadata,  #
#     Column('ID', Integer, Identity(start=1), primary_key=True, comment="主键"),
#     Column('USERROLE_ID', Integer, ForeignKey("T_W_BJHY_ROLE.rid", ondelete='CASCADE'), primary_key=True),
#     Column('USERPERMISSION_ID', Integer, ForeignKey("T_W_BJHY_PERMISSION.pid", ondelete='CASCADE'), primary_key=True),
# )


class Permission(Base):
    __tablename__ = "T_W_BJHY_PERMISSION"
    pid = Column(Integer, Identity(start=1), primary_key=True, comment="主键")
    ptype = Column(Integer, nullable=False, unique=True, comment="权限类型")
    permissionname = Column(String(256), nullable=False, unique=True, comment="权限名称")
    method = Column(String(10), nullable=True, comment="请求方法")
    url = Column(String(200), nullable=True, comment="请求url")
    permission_status = Column(Boolean, default=False, comment="状态")
    ctime = Column(DateTime, nullable=True, comment="创建日期", server_default=func.now())
    uptime = Column(DateTime, nullable=True, comment="更新日期", server_default=func.now(), server_onupdate=func.now())
    description = Column(String(200), nullable=True, comment="描述")

    def __repr__(self):
        return f"permissionname:{self.permissionname}"


class UserRolePermission(Base):
    __tablename__ = "T_W_BJHY_ROLE_PERMISSION"
    ID = Column(Integer, Identity(start=1), primary_key=True, comment="主键")
    USERROLE_ID = Column(Integer, ForeignKey("T_W_BJHY_ROLE.rid", ondelete='CASCADE'), comment="外键信息rid")
    USERPERMISSION_ID = Column(Integer, ForeignKey("T_W_BJHY_PERMISSION.pid", ondelete='CASCADE'), comment="外键信息pid")

    def __repr__(self):
        return f"ID:{self.ID}"


class UserRole(Base):
    __tablename__ = "T_W_BJHY_ROLE"
    rid = Column(Integer, Identity(start=1), primary_key=True, comment="主键")
    rolename = Column(String(200), nullable=True, unique=True)
    description = Column(String(200), nullable=True)
    ctime = Column(DateTime, nullable=True, comment="创建日期", server_default=func.now())
    uptime = Column(DateTime, nullable=True, comment="更新日期", server_default=func.now(), server_onupdate=func.now())
    # permission = relationship("Permission", backref="roles", secondary=permission_role)
    permission = relationship("Permission", backref="roles", secondary=UserRolePermission.__table__)

    # secondary = HostToGroup.__table__,
    def __repr__(self):
        return f"rolename:{self.rolename}"


class User(Base):
    __tablename__ = "T_W_BJHY_USERS"
    # __table_args__ = {'mysql_engine': 'InnoDB'}
    # userid = Column(Integer, Sequence(__tablename__ + '_ID_SEQ', start=100, increment=1), primary_key=True)
    userid = Column(Integer, Identity(start=1), primary_key=True, comment="主键")
    username = Column(String(256), nullable=True, unique=True)
    useremail = Column(String(100), nullable=False, unique=True)
    user_active = Column(Boolean, default=False)
    userhashed_password = Column(String(300), nullable=False)
    userphonenum = Column(String(32), doc="手机号")
    ctime = Column(DateTime, nullable=True, comment="创建日期", server_default=func.now())
    uptime = Column(DateTime, nullable=True, comment="更新日期", server_default=func.now(), server_onupdate=func.now())
    role_id = Column(Integer, ForeignKey("T_W_BJHY_ROLE.rid", ondelete='CASCADE'), comment="外键信息id")
    role = relationship("UserRole", backref="user")

    def __repr__(self):
        return f"userid:{self.userid},username:{self.username},useremail:{self.useremail},user_active:{self.user_active}" \
               f",userhashed_password:{self.userhashed_password},userphonenum:{self.userphonenum}"


if __name__ == "__main__":
    import cx_Oracle
    import pymysql
    from sqlalchemy import create_engine

    # pymysql.install_as_MySQLdb()

    os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
    dns = cx_Oracle.makedsn('192.168.101.215', 1521, service_name='bjhy')
    # engine = create_engine("oracle://fcdb:fcdb@" + dns, encoding='utf-8', echo=True)
    engine = create_engine("oracle://bjhy:bjhy@" + dns, encoding='utf-8', echo=True)
    # engine = create_engine("mysql://root:1qaz!QAZ@192.168.101.205/udb?charset=utf8")
    Base.metadata.create_all(engine)
