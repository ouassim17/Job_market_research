from celery import Celery

app = Celery("celery_app")
default_config = "celery_app.celeryconfig"
app.config_from_object(default_config)
