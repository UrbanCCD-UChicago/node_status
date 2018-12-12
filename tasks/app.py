from celery import Celery
from config import Config
from datetime import timedelta

_cfg = Config()
app = Celery('tasks', broker=_cfg.get_rmq_dsn(), include=[
    'expire_node_last_hour', 'expire_observations', 'load_data', 'load_http_events', 'load_unresponsive_nodes'])

app.conf.timezone = 'UTC'
app.conf.beat_schedule = {
    'expire_node_last_hour': {
        'task': 'expire_node_last_hour.expire_node_last_hour',
        'schedule': timedelta(minutes=1)
    },
    'expire_observations': {
        'task': 'expire_observations.expire_observations',
        'schedule': timedelta(minutes=1)
    },
    'load_data': {
        'task': 'load_data.load_data',
        'schedule': timedelta(minutes=5)
    },
    'load_http_events': {
        'task': 'load_http_events.load_http_events',
        'schedule': timedelta(minutes=30)
    },
    'load_unresponsive_nodes': {
        'task': 'load_unresponsive_nodes.load_unresponsive_nodes',
        'schedule': timedelta(minutes=30)
    }
}