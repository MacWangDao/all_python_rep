from django.apps import AppConfig


class AdminModelConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'AdminModel'
    # verbose_name = "认证和授权"
    verbose_name = "数据资源"
