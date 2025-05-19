from celery import Celery

app = Celery(
    "celery_app", broker="redis://redis:6379/0", backend="redis://redis:6379/0"
)

result = app.send_task("tasks.add", args=[3, 4])
print(result.get(timeout=10))
