from os import environ as env
from os import getenv


import celery
from celery import Celery
from celery.schedules import crontab
from celery.signals import worker_ready
from .loggers import full_logger

LOG = full_logger("demo_work")

BROKER_PROTO = getenv('BROKER_PROTO', 'amqp')
BROKER_PORT = getenv('BROKER_PORT', '5672')
BACKEND_PROTO = 'rpc'
d = {}

DEFAULT_QUEUE_NAME = 'dht'
DEFAULT_EXCHANGE_NAME = 'dht'
# DEFAULT_ROUTING_KEY = 'dht'
# INVENTORY_QUEUE_NAME = 'dht_inv'
# INVENTORY_ROUTING_KEY = 'dht_inv'

broker_url = f'{BROKER_PROTO}://{env["BROKER_USER"]}:{env["BROKER_PASS"]}@{env["BROKER_HOST"]}:{BROKER_PORT}/'

backend_url = f'{BACKEND_PROTO}://{env["BROKER_USER"]}:{env["BROKER_PASS"]}@{env["BROKER_HOST"]}:{BROKER_PORT}/'

app = Celery('task',
             broker=broker_url,
             backend=backend_url,
             include=['demo_task.tasks'])

# Optional configuration, see the application user guide.
# logger = full_logger("daily_health_data_transfer")

app.conf.broker_pool_limit = int(getenv('POOL_SIZE', 50))


app.conf.update(
    result_expires=1800,
    imports=['demo_task.tasks'],
    task_routes=([
                     ('demo_task.tasks.kickoff_startup', {'queue': DEFAULT_QUEUE_NAME})
                     # ('demo_task.tasks.process_latest_inventory', {'queue': INVENTORY_QUEUE_NAME}),
                 ],),
    worker_hijack_root_logger=False,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    broker_pool_limit=int(getenv('POOL_SIZE', 5)),
    worker_redirect_stdouts=False,
)

app.autodiscover_tasks()
app.conf.beat_schedule = {
    "DHT_RUN_EVERYDAY_4": {
        "task": "demo_task.tasks.kickoff_scheduled",
        "schedule": crontab(hour=4, minute=0)
    }
}
app.conf.timezone = 'America/New_York'

app.conf.beat_startup_tasks = []
app.config_from_object(__name__)


@celery.signals.celeryd_after_setup.connect
def on_startup(sender, *arg, **kwargs):
    from .tasks import kickoff_startup
    i = 0
    for i in range(2):
        kickoff_startup.delay()
