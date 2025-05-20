from celery.utils.log import get_task_logger

from celery_app import app
from data_extraction.Websites import Rekrute

logger = get_task_logger(__name__)


@app.task(
    name="add",
)
def add(x, y):
    logger.info("Adding {0} + {1}".format(x, y))
    return x + y


@app.task(
    name="rekrute",
)
def rekrute():
    try:
        print("Appel du script rekrute")
        return Rekrute.main()
    except Exception:
        print("Exception lors de l'execution")
