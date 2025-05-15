from celery import Celery

app = Celery("tasks", broker="redis://localhost:6379", backend="redis://localhost:6379")


@app.task(name="tasks.add")
def add(x, y):
    return x + y


@app.task(name="tasks.mult")
def mult(x, y):
    return x * y


@app.task(name="tasks.sub")
def sub(x, y):
    return x - y
