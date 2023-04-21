from django.contrib.auth.models import AbstractUser
from django.db import models


# Create your models here.
# class Contact(models.Model):
#     name = models.CharField(max_length=200)
#     age = models.IntegerField(default=0)
#     email = models.EmailField()
#
#     def __unicode__(self):
#         return self.name
#
#     class Meta:
#         db_table = 'tb_contact'
#         verbose_name = '内容'
#         verbose_name_plural = verbose_name


class Host(models.Model):
    hid = models.AutoField(primary_key=True, verbose_name='ID')
    hostname = models.CharField(max_length=200, unique=True, verbose_name='配置名称')
    oracle_host = models.CharField(max_length=20, verbose_name='Oracle IP')
    oracle_port = models.IntegerField(verbose_name='Oracle端口', default=1521)
    oracle_user = models.CharField(max_length=30, verbose_name='Oracle用户名')
    oracle_password = models.CharField(max_length=30, verbose_name='Oracle密码')
    oracle_service_name = models.CharField(max_length=30, verbose_name='Oracle服务名称')
    redis_host = models.CharField(max_length=20, verbose_name='redis IP')
    redis_port = models.IntegerField(verbose_name='redis端口', default=6379)
    redis_db = models.IntegerField(verbose_name='redis数据库', default=0)
    kafka_bootstrap_servers = models.CharField(max_length=200, verbose_name='kafka_bootstrap_servers', default="")
    kafka_topic_req_pipe = models.CharField(max_length=20, verbose_name='kafka_topic_req_pipe', default="")
    kafka_topic_rsp_pipe = models.CharField(max_length=20, verbose_name='kafka_topic_rsp_pipe', default="")
    kafka_topic_command = models.CharField(max_length=20, verbose_name='kafka_topic_command', default="")
    zmq_host = models.CharField(max_length=15, verbose_name='zmq_host', default="")
    zmq_port = models.IntegerField(verbose_name='zmq_port', default=8046)
    influxdb_url = models.CharField(max_length=100, verbose_name='influxdb_url', default="")
    influxdb_token = models.CharField(max_length=200, verbose_name='influxdb_token', default="")
    influxdb_org = models.CharField(max_length=15, verbose_name='influxdb_org', default="")
    influxdb_bucket = models.CharField(max_length=20, verbose_name='influxdb_bucket', default="")
    twap_limit_count = models.IntegerField(verbose_name='twap_limit_count', default=2)
    pov_limit_count = models.IntegerField(verbose_name='pov_limit_count', default=2)
    pov_limit_sub_count = models.IntegerField(verbose_name='pov_limit_sub_count', default=2)
    description = models.CharField(max_length=200, verbose_name='描述')
    securitytype = models.IntegerField(verbose_name='securitytype', default=2)
    authid = models.CharField(max_length=20, verbose_name='authid')
    authcode = models.CharField(max_length=200, verbose_name='authcode')
    ip = models.CharField(max_length=20, verbose_name='ip')
    port = models.IntegerField(verbose_name='port', default=32030)
    localip = models.CharField(max_length=20, verbose_name='localip')
    mac = models.CharField(max_length=30, verbose_name='mac')
    pcname = models.CharField(max_length=30, verbose_name='pcname')
    diskid = models.CharField(max_length=30, verbose_name='diskid')
    cpuid = models.CharField(max_length=30, verbose_name='cpuid')
    pi = models.CharField(max_length=20, verbose_name='pi')
    vol = models.CharField(max_length=20, verbose_name='vol')
    clientname = models.CharField(max_length=100, verbose_name='clientname')
    clientversion = models.CharField(max_length=20, verbose_name='clientversion')
    status = models.BooleanField(default=False, verbose_name='状态')

    class Meta:
        db_table = 'T_W_BJHY_HOST'
        verbose_name = '配置管理'
        verbose_name_plural = verbose_name

    def __str__(self):
        return self.hostname


class User(AbstractUser):
    """用户模型类
    创建自定义的用户模型类
    Django认证系统中提供的用户模型类及方法很方便，我们可以使用这个模型类，但是字段有些无法满足项目需求，如本项目中需要保存用户的手机号，
    需要给模型类添加额外的字段。
    Django提供了django.contrib.auth.models.AbstractUser用户抽象模型类允许我们继承，扩展字段来使用Django认证系统的用户模型类。
    我们自定义的用户模型类还不能直接被Django的认证系统所识别，需要在配置文件中告知Django认证系统使用我们自定义的模型类。
    在配置文件中进行设置
    AUTH_USER_MODEL = 'users.User'
    AUTH_USER_MODEL 参数的设置以点.来分隔，表示应用名.模型类名。
    """
    mobile = models.CharField(max_length=11, unique=True, verbose_name='手机号')
    address = models.CharField(max_length=100, unique=False, verbose_name='详细地址', null=True)
    nickname = models.CharField(max_length=50, verbose_name='昵称', null=True)

    class Meta:
        db_table = 'T_W_BJHY_ADMIN_USERS'
        verbose_name = '超级管理员用户管理'
        verbose_name_plural = verbose_name

    def __str__(self):  # 必需有值的字段,当前对象的描写
        return self.username  # 返回此对象的用户名username


class UserPermission(models.Model):
    pid = models.AutoField(primary_key=True, verbose_name='ID')
    ptype = models.IntegerField(verbose_name='权限类型')
    permissionname = models.CharField(max_length=256, unique=True, verbose_name='权限名称')
    method = models.CharField(max_length=10, verbose_name='请求方法')
    url = models.CharField(max_length=200, verbose_name='请求url')
    permission_status = models.BooleanField(default=False, verbose_name='状态')
    ctime = models.DateTimeField(auto_now=True, verbose_name='更新日期')
    uptime = models.DateTimeField(auto_now_add=True, verbose_name='创建日期')
    description = models.CharField(max_length=200, verbose_name='描述')

    class Meta:
        db_table = 'T_W_BJHY_PERMISSION'
        verbose_name = '用户权限管理'
        verbose_name_plural = verbose_name

    def __str__(self):  # 必需有值的字段,当前对象的描写
        return self.permissionname  # 返回此对象的用户名username


class UserRole(models.Model):
    rid = models.AutoField(primary_key=True, verbose_name='ID')
    rolename = models.CharField(max_length=200, unique=True, verbose_name='角色名称')
    permission = models.ManyToManyField(to=UserPermission, related_name="p_roles", blank=True)
    description = models.CharField(max_length=200, verbose_name='备注')
    ctime = models.DateTimeField(auto_now=True, verbose_name='更新日期')
    uptime = models.DateTimeField(auto_now_add=True, verbose_name='创建日期')

    class Meta:
        db_table = 'T_W_BJHY_ROLE'
        verbose_name = '角色管理'
        verbose_name_plural = verbose_name

    def __str__(self):
        return self.rolename

    def __unicode__(self):
        return self.rolename

    # def display_permissionname(self):
    #     return ','.join([permission.permissionname for permission in self.permission.all()])
    #
    # display_permissionname.short_description = 'Permissionname'


class LoginUser(models.Model):
    userid = models.AutoField(primary_key=True, verbose_name='ID')
    username = models.CharField(max_length=256, unique=True, verbose_name='用户名')
    useremail = models.EmailField(verbose_name='邮箱')
    userhashed_password = models.CharField(max_length=300, unique=False, verbose_name='密码')
    userphonenum = models.CharField(max_length=300, unique=False, verbose_name='手机号')
    user_active = models.BooleanField(default=False, verbose_name='激活状态')
    role = models.ForeignKey(UserRole, on_delete=models.CASCADE, related_query_name='roles', verbose_name='角色')
    # role = models.ForeignKey(Role, to_field=Role.rid, on_delete=models.CASCADE)
    ctime = models.DateTimeField(auto_now=True, verbose_name='更新日期')
    uptime = models.DateTimeField(auto_now_add=True, verbose_name='创建日期')

    class Meta:
        db_table = 'T_W_BJHY_USERS'
        verbose_name = '交易员账户管理'
        verbose_name_plural = verbose_name

    def __str__(self):  # 必需有值的字段,当前对象的描写
        return self.username  # 返回此对象的用户名username


class AccountInfo(models.Model):
    acid = models.AutoField(primary_key=True, verbose_name='ID')
    userid = models.ForeignKey(LoginUser, on_delete=models.CASCADE, related_query_name='users', verbose_name='用户id')
    uname = models.CharField(max_length=200, unique=False, verbose_name='用户名')
    usernum = models.CharField(max_length=200, unique=False, verbose_name='用户号')
    account = models.CharField(max_length=200, unique=False, verbose_name='账号')
    password = models.CharField(max_length=200, unique=False, verbose_name='密码')
    status = models.BooleanField(default=False, verbose_name='状态')
    description = models.CharField(max_length=200, verbose_name='备注')
    # host = models.ForeignKey(Host, on_delete=models.CASCADE, related_query_name='host', verbose_name='节点')
    host = models.ForeignKey('Host', to_field='hostname', on_delete=models.CASCADE, verbose_name='节点')
    ctime = models.DateTimeField(auto_now=True, verbose_name='更新日期')
    uptime = models.DateTimeField(auto_now_add=True, verbose_name='创建日期')

    class Meta:
        db_table = 'T_W_BJHY_ACCOUNTINFOS'
        verbose_name = '资金账号管理'
        verbose_name_plural = verbose_name

    def __str__(self):
        return self.uname

    def __unicode__(self):
        return self.uname
