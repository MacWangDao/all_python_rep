class PermissionChecker:
    """
    权限管理的类型，把闭包改为类实现
    """

    def __init__(self, permissions: Optional[List[str]]):
        """
        传递需要验证是否具有的权限的列表。
        :param permissions:
        """
        self.permissions = permissions

    async def __call__(self, user: User = Depends(user_has_login)) -> User:
        """
        生成的依赖函数
        """
        if user.is_superuser:
            return user
        if not self.permissions:
            return user
        for need_permission in self.permissions:
            db = AdminDatabase().database  # 通过单例实现的全局数据库调用类
            # 用户->group，group和permission多对多，由auth_group_permission表记录多对多关系字段
            query = select([auth_group_permission]).where(auth_group_permission.c.group_id == user.group_id).where(
                auth_group_permission.c.codename == need_permission)  # 这个主要是数据库查询是否有权限的过程，由于我使用的异步数据库查询，所以这里没有直接用sqlalchemy的默认查询方式
            res = await db.fetch_all(query)
            if not res:
                raise HTTPException(status_code=403, detail="没有权限")
        return user


def func_user_has_permissions(need_permissions: List[str] = None) -> Callable:
    """
    生成权限认证的依赖
    """

    async def user_has_permission(user: User = Depends(user_has_login)) -> User:
        """
        是否有某权限
        """
        if user.is_superuser:
            return user
        if not need_permissions:
            return user
        for need_permission in need_permissions:
            db = AdminDatabase().database
            query = select([auth_group_permission]).where(auth_group_permission.c.group_id == user.group_id).where(
                auth_group_permission.c.codename == need_permission)
            res = await db.fetch_all(query)
            if not res:
                raise HTTPException(status_code=403, detail="没有权限")
        return user

    return user_has_permission
