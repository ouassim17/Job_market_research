broker_url = "redis://redis:6379"
result_backend = "redis://redis:6379"
worker_send_task_events = True  # to use flower event monitoring
events_logfile = "celery.log"
events_pidfile = "celery.pid"
