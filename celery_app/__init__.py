from celery import Celery

# Names the app "celery_app"
app = Celery("celery_app")
# path to the default config for "celery_app"
default_config = "celery_app.celeryconfig"
app.config_from_object(default_config)
