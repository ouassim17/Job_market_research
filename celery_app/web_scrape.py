from celery_app.tasks import bayt_task, emploi_task, marocann_task, rekrute_task

rekrute_task.delay()
bayt_task.delay()
emploi_task.delay()
marocann_task.delay()
