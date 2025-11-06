from django.apps import AppConfig


class BaseConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'base'

    def ready(self):
        # Import signal handlers that maintain the data backbone automatically.
        from . import signals  # noqa: F401
