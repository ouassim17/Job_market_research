broker_url = "redis://redis:6379/0"
result_backend = "redis://redis:6379/0"
worker_send_task_events = True  # to use flower event monitoring
events_logfile = "celery.log"
events_pidfile = "celery.pid"
