import json

from django.contrib import admin

# Register your models here.
from django.contrib.auth.admin import UserAdmin
from django.contrib.auth.models import Group
from django.forms import model_to_dict
from django_redis import get_redis_connection

from AdminModel.models import User, LoginUser, UserRole, Host, UserPermission, AccountInfo
from django.utils.translation import gettext_lazy
from passlib.context import CryptContext

admin.site.site_header = '用户交易管理系统'  # 设置header
admin.site.site_title = 'OSS'  # 设置title
admin.site.index_title = "后台主页"
PWD_CONTEXT = CryptContext(schemes=["bcrypt"], deprecated="auto")

admin.site.unregister(Group)


# admin.site.unregister(User)
@admin.register(User)
class UserProfileAdmin(UserAdmin):
    list_display = ['id', 'username', 'is_superuser', 'is_staff', 'is_active', 'mobile', 'email',
                    'address', 'date_joined']  # 展示页面显示字段设置
    ordering = ['id']
    list_per_page = 10  # 展示页面展示的条
    # ordering = ['username', 'mobile']
    list_editable = ('username', 'email', 'mobile', 'address', 'is_superuser', 'is_staff', 'is_active')
    search_fields = ['username', 'mobile']
    # 增加用户时密码为密文
    add_fieldsets = (
        (None, {u'fields': ('username', 'password1', 'password2')}),
        (gettext_lazy('User Information'),
         {'fields': ('nickname', 'address', 'mobile', 'first_name', 'last_name')}),
        # 增加页面显示字段设置
        (gettext_lazy('Permissions'), {'fields': ('is_superuser', 'is_staff', 'is_active',
                                                  'groups', 'user_permissions')}),
        (gettext_lazy('Important dates'), {'fields': ('last_login', 'date_joined')})
    )


# @admin.register(Contact)
# class ContactProfileAdmin(admin.ModelAdmin):
#     def save_model(self, request, obj, form, change):
#         # obj.user = request.user
#         obj.age = obj.age + 10
#         super().save_model(request, obj, form, change)

# @admin.register(User)
# class UserProfileAdmin(UserAdmin):
#     pass
# @admin.register(User)
# class UserProfileAdmin(UserAdmin):
#     pass


def get_password_hash(password: str) -> str:
    return PWD_CONTEXT.hash(password)


@admin.register(LoginUser)
class LoginUserProfileAdmin(admin.ModelAdmin):
    list_display = ['userid', 'username', 'useremail', 'userphonenum', 'role', 'user_active', 'ctime',
                    'uptime']  # 展示页面显示字段设置
    ordering = ['userid']
    list_per_page = 10  # 展示页面展示的条
    # ordering = ['username', 'userphonenum']
    # list_editable = ('username', 'useremail', 'userphonenum', 'role', 'user_active')
    list_display_links = ('username',)
    search_fields = ['username', 'userphonenum']

    # def view_role_name(self, obj):
    #     return obj.role.rolename

    # 增加用户时密码为密文
    # add_fieldsets = (
    #     (None, {u'fields': ('username', 'password1', 'password2')}),
    #     (gettext_lazy('User Information'),
    #      {'fields': ('nickname', 'address', 'mobile', 'first_name', 'last_name')}),
    #     # 增加页面显示字段设置
    #     (gettext_lazy('Permissions'), {'fields': ('is_superuser', 'is_staff', 'is_active',
    #                                               'groups', 'user_permissions')}),
    #     (gettext_lazy('Important dates'), {'fields': ('last_login', 'date_joined')})
    # )

    def save_model(self, request, obj, form, change):
        conn = get_redis_connection("default")
        permissions = []
        if not change:
            obj.userhashed_password = get_password_hash(obj.userhashed_password)
        else:
            riginal_obj = self.model.objects.get(pk=obj.pk)
            original_data = model_to_dict(riginal_obj)
            new_data = model_to_dict(obj)
            for field, value in new_data.items():
                if value != original_data.get(field):
                    if field == "userhashed_password":
                        obj.userhashed_password = get_password_hash(value)
                    else:
                        pass
                else:
                    if field == "userhashed_password":
                        obj.userhashed_password = original_data.get(field)

        super().save_model(request, obj, form, change)
        role_id = obj.role.rid
        permission_ds = obj.role.permission.all()
        for item_data in permission_ds:
            if item_data.permission_status:
                permissions.append(item_data.ptype)
        conn.hset(obj.userid, "permissions", json.dumps(permissions))
        conn.hset(obj.userid, "role_id", role_id)
        conn.hset(obj.userid, "userid", obj.userid)
        conn.hset(obj.userid, "username", obj.username)


@admin.register(UserRole)
class RoleProfileAdmin(admin.ModelAdmin):
    def display_permissionname(self, obj):
        return [pnm.permissionname for pnm in obj.permission.all()]

    display_permissionname.short_description = '权限'
    list_display = ['rid', 'rolename', 'description', 'display_permissionname', 'ctime', 'uptime']
    list_display_links = ('rolename',)
    ordering = ['rid']
    list_per_page = 10
    search_fields = ['rolename']
    # list_editable = ('rolename', 'description')
    fieldsets = (
        (None, {'fields': ('rolename', 'permission', 'description')}),
    )
    filter_horizontal = ('permission',)

    def save_model(self, request, obj, form, change):

        # username = conn.hgetall(user.userid)
        # permissions = conn.hget(user.userid, "permissions")
        # permissions = json.loads(permissions)
        # print(permissions)

        # if not change:
        #     obj.userhashed_password = get_password_hash(obj.userhashed_password)
        if change:
            user = LoginUser.objects.get(role=obj.rid)
            conn = get_redis_connection("default")
            permissions = []
            permission_ds = form.cleaned_data["permission"]
            for item_data in permission_ds:
                if item_data.permission_status:
                    if item_data.ptype not in permissions:
                        permissions.append(item_data.ptype)
            conn.hset(user.userid, "permissions", json.dumps(permissions))
        super().save_model(request, obj, form, change)


@admin.register(Host)
class HostProfileAdmin(admin.ModelAdmin):
    list_display = ['hid', 'hostname', 'status', 'kafka_bootstrap_servers', 'oracle_host', 'redis_host', 'zmq_host']
    list_display_links = ('hostname',)
    ordering = ['hid']
    search_fields = ['hostname']
    list_per_page = 10


@admin.register(UserPermission)
class PermissionProfileAdmin(admin.ModelAdmin):
    list_display = ['pid', 'permissionname', 'ptype', 'method', 'url', 'permission_status', 'ctime', 'uptime']
    list_display_links = ('permissionname',)
    ordering = ['pid']
    list_per_page = 10
    search_fields = ['rolename']


@admin.register(AccountInfo)
class AccountInfoProfileAdmin(admin.ModelAdmin):
    list_display = ['acid', 'uname', 'usernum', 'account', 'password', 'status', 'host', 'description', 'ctime',
                    'uptime']
    list_display_links = ('uname',)
    ordering = ['acid']
    list_per_page = 10
    search_fields = ['uname', 'usernum', 'account']
