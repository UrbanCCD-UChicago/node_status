from celery import Celery
from config import Config
from datetime import timedelta

_cfg = Config()
app = Celery('tasks', broker=_cfg.get_rmq_dsn(), include=[
    'expire_observations', 'load_nodes_and_data', 'load_boot_events', 'load_rssh_ports'])

app.conf.timezone = 'UTC'
app.conf.beat_schedule = {
    'expire_observations': {
        'task': 'expire_observations.run',
        'schedule': timedelta(minutes=5)
    },
    'load_nodes_and_data': {
        'task': 'load_nodes_and_data.run',
        'schedule': timedelta(minutes=5)
    },
    'load_boot_events': {
        'task': 'load_boot_events.run',
        'schedule': timedelta(minutes=5)
    },
    'load_rssh_ports': {
        'task': 'load_rssh_ports.run',
        'schedule': timedelta(minutes=5)
    }
}