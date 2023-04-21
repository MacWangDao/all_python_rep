# Generated by Django 4.1.3 on 2023-01-04 02:16

import django.contrib.auth.models
import django.contrib.auth.validators
from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ('auth', '0012_alter_user_first_name_max_length'),
    ]

    operations = [
        migrations.CreateModel(
            name='Host',
            fields=[
                ('hid', models.AutoField(primary_key=True, serialize=False, verbose_name='ID')),
                ('hostname', models.CharField(max_length=200, unique=True, verbose_name='配置名称')),
                ('oracle_host', models.CharField(max_length=20, verbose_name='Oracle IP')),
                ('oracle_port', models.IntegerField(default=1251, verbose_name='Oracle端口')),
                ('oracle_user', models.CharField(max_length=30, verbose_name='Oracle用户名')),
                ('oracle_password', models.CharField(max_length=30, verbose_name='Oracle密码')),
                ('oracle_service_name', models.CharField(max_length=30, verbose_name='Oracle服务名称')),
                ('redis_host', models.CharField(max_length=20, verbose_name='redis IP')),
                ('redis_port', models.IntegerField(default=6379, verbose_name='redis端口')),
                ('redis_db', models.IntegerField(default=0, verbose_name='redis数据库')),
                ('kafka_bootstrap_servers', models.CharField(default='', max_length=200, verbose_name='kafka_bootstrap_servers')),
                ('kafka_topic_req_pipe', models.CharField(default='', max_length=20, verbose_name='kafka_topic_req_pipe')),
                ('kafka_topic_rsp_pipe', models.CharField(default='', max_length=20, verbose_name='kafka_topic_rsp_pipe')),
                ('kafka_topic_command', models.CharField(default='', max_length=20, verbose_name='kafka_topic_command')),
                ('zmq_host', models.CharField(default='', max_length=15, verbose_name='zmq_host')),
                ('zmq_port', models.IntegerField(default=8046, verbose_name='zmq_port')),
                ('influxdb_url', models.CharField(default='', max_length=100, verbose_name='influxdb_url')),
                ('influxdb_token', models.CharField(default='', max_length=200, verbose_name='influxdb_token')),
                ('influxdb_org', models.CharField(default='', max_length=15, verbose_name='influxdb_org')),
                ('influxdb_bucket', models.CharField(default='', max_length=20, verbose_name='influxdb_bucket')),
                ('twap_limit_count', models.IntegerField(default=2, verbose_name='twap_limit_count')),
                ('pov_limit_count', models.IntegerField(default=2, verbose_name='pov_limit_count')),
                ('pov_limit_sub_count', models.IntegerField(default=2, verbose_name='pov_limit_sub_count')),
                ('description', models.CharField(max_length=200, verbose_name='描述')),
                ('securitytype', models.IntegerField(default=2, verbose_name='securitytype')),
                ('authid', models.CharField(max_length=20, verbose_name='authid')),
                ('authcode', models.CharField(max_length=200, verbose_name='authcode')),
                ('ip', models.CharField(max_length=20, verbose_name='ip')),
                ('port', models.IntegerField(default=32030, verbose_name='port')),
                ('localip', models.CharField(max_length=20, verbose_name='localip')),
                ('mac', models.CharField(max_length=30, verbose_name='mac')),
                ('pcname', models.CharField(max_length=30, verbose_name='pcname')),
                ('diskid', models.CharField(max_length=30, verbose_name='diskid')),
                ('cpuid', models.CharField(max_length=30, verbose_name='cpuid')),
                ('pi', models.CharField(max_length=20, verbose_name='pi')),
                ('vol', models.CharField(max_length=20, verbose_name='vol')),
                ('clientname', models.CharField(max_length=100, verbose_name='clientname')),
                ('clientversion', models.CharField(max_length=20, verbose_name='clientversion')),
            ],
            options={
                'verbose_name': '配置管理',
                'verbose_name_plural': '配置管理',
                'db_table': 'T_W_BJHY_HOST',
            },
        ),
        migrations.CreateModel(
            name='UserPermission',
            fields=[
                ('pid', models.AutoField(primary_key=True, serialize=False, verbose_name='ID')),
                ('ptype', models.IntegerField(verbose_name='权限类型')),
                ('permissionname', models.CharField(max_length=256, unique=True, verbose_name='权限名称')),
                ('method', models.CharField(max_length=10, verbose_name='请求方法')),
                ('url', models.CharField(max_length=200, verbose_name='请求url')),
                ('permission_status', models.BooleanField(default=False, verbose_name='状态')),
                ('ctime', models.DateTimeField(auto_now=True, verbose_name='更新日期')),
                ('uptime', models.DateTimeField(auto_now_add=True, verbose_name='创建日期')),
                ('description', models.CharField(max_length=200, verbose_name='描述')),
            ],
            options={
                'verbose_name': '用户权限管理',
                'verbose_name_plural': '用户权限管理',
                'db_table': 'T_W_BJHY_PERMISSION',
            },
        ),
        migrations.CreateModel(
            name='User',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('password', models.CharField(max_length=128, verbose_name='password')),
                ('last_login', models.DateTimeField(blank=True, null=True, verbose_name='last login')),
                ('is_superuser', models.BooleanField(default=False, help_text='Designates that this user has all permissions without explicitly assigning them.', verbose_name='superuser status')),
                ('username', models.CharField(error_messages={'unique': 'A user with that username already exists.'}, help_text='Required. 150 characters or fewer. Letters, digits and @/./+/-/_ only.', max_length=150, unique=True, validators=[django.contrib.auth.validators.UnicodeUsernameValidator()], verbose_name='username')),
                ('first_name', models.CharField(blank=True, max_length=150, verbose_name='first name')),
                ('last_name', models.CharField(blank=True, max_length=150, verbose_name='last name')),
                ('email', models.EmailField(blank=True, max_length=254, verbose_name='email address')),
                ('is_staff', models.BooleanField(default=False, help_text='Designates whether the user can log into this admin site.', verbose_name='staff status')),
                ('is_active', models.BooleanField(default=True, help_text='Designates whether this user should be treated as active. Unselect this instead of deleting accounts.', verbose_name='active')),
                ('date_joined', models.DateTimeField(default=django.utils.timezone.now, verbose_name='date joined')),
                ('mobile', models.CharField(max_length=11, unique=True, verbose_name='手机号')),
                ('address', models.CharField(max_length=100, null=True, verbose_name='详细地址')),
                ('nickname', models.CharField(max_length=50, null=True, verbose_name='昵称')),
                ('groups', models.ManyToManyField(blank=True, help_text='The groups this user belongs to. A user will get all permissions granted to each of their groups.', related_name='user_set', related_query_name='user', to='auth.group', verbose_name='groups')),
                ('user_permissions', models.ManyToManyField(blank=True, help_text='Specific permissions for this user.', related_name='user_set', related_query_name='user', to='auth.permission', verbose_name='user permissions')),
            ],
            options={
                'verbose_name': '超级管理员用户管理',
                'verbose_name_plural': '超级管理员用户管理',
                'db_table': 'T_W_BJHY_ADMIN_USERS',
            },
            managers=[
                ('objects', django.contrib.auth.models.UserManager()),
            ],
        ),
        migrations.CreateModel(
            name='UserRole',
            fields=[
                ('rid', models.AutoField(primary_key=True, serialize=False, verbose_name='ID')),
                ('rolename', models.CharField(max_length=200, unique=True, verbose_name='角色名称')),
                ('description', models.CharField(max_length=200, verbose_name='备注')),
                ('ctime', models.DateTimeField(auto_now=True, verbose_name='更新日期')),
                ('uptime', models.DateTimeField(auto_now_add=True, verbose_name='创建日期')),
                ('permission', models.ManyToManyField(blank=True, related_name='p_roles', to='AdminModel.userpermission')),
            ],
            options={
                'verbose_name': '角色管理',
                'verbose_name_plural': '角色管理',
                'db_table': 'T_W_BJHY_ROLE',
            },
        ),
        migrations.CreateModel(
            name='LoginUser',
            fields=[
                ('userid', models.AutoField(primary_key=True, serialize=False, verbose_name='ID')),
                ('username', models.CharField(max_length=256, unique=True, verbose_name='用户名')),
                ('useremail', models.EmailField(max_length=254, verbose_name='邮箱')),
                ('userhashed_password', models.CharField(max_length=300, verbose_name='密码')),
                ('userphonenum', models.CharField(max_length=300, verbose_name='手机号')),
                ('user_active', models.BooleanField(default=False, verbose_name='激活状态')),
                ('ctime', models.DateTimeField(auto_now=True, verbose_name='更新日期')),
                ('uptime', models.DateTimeField(auto_now_add=True, verbose_name='创建日期')),
                ('role', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_query_name='roles', to='AdminModel.userrole', verbose_name='角色')),
            ],
            options={
                'verbose_name': '交易员账户管理',
                'verbose_name_plural': '交易员账户管理',
                'db_table': 'T_W_BJHY_USERS',
            },
        ),
        migrations.CreateModel(
            name='AccountInfo',
            fields=[
                ('acid', models.AutoField(primary_key=True, serialize=False, verbose_name='ID')),
                ('uname', models.CharField(max_length=200, verbose_name='用户名')),
                ('usernum', models.CharField(max_length=200, verbose_name='用户号')),
                ('account', models.CharField(max_length=200, verbose_name='账号')),
                ('password', models.CharField(max_length=200, verbose_name='密码')),
                ('status', models.BooleanField(default=False, verbose_name='状态')),
                ('description', models.CharField(max_length=200, verbose_name='备注')),
                ('ctime', models.DateTimeField(auto_now=True, verbose_name='更新日期')),
                ('uptime', models.DateTimeField(auto_now_add=True, verbose_name='创建日期')),
                ('host', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='AdminModel.host', to_field='hostname', verbose_name='节点')),
                ('userid', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_query_name='users', to='AdminModel.loginuser', verbose_name='用户id')),
            ],
            options={
                'verbose_name': '资金账号管理',
                'verbose_name_plural': '资金账号管理',
                'db_table': 'T_W_BJHY_ACCOUNTINFOS',
            },
        ),
    ]
