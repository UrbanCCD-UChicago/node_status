import os

from logging2 import LogLevel

class __BaseConfig:
    pg_host   = os.environ.get('POSTGRES_HOST',     'localhost')
    pg_port   = os.environ.get('POSTGRES_PORT',     '5432')
    pg_user   = os.environ.get('POSTGRES_USER',     'postgres')
    pg_pass   = os.environ.get('POSTGRES_PASSWORD', 'postgres')
    pg_dbname = os.environ.get('POSTGRES_DB',       'postgres')

    rmq_host  = os.environ.get('RABBITMQ_HOST',          'localhost')
    rmq_port  = os.environ.get('RABBITMQ_PORT',          '5672')
    rmq_user  = os.environ.get('RABBITMQ_DEFAULT_USER',  'guest')
    rmq_pass  = os.environ.get('RABBITMQ_DEFAULT_PASS',  'guest')
    rmq_vhost = os.environ.get('RABBITMQ_DEFAULT_VHOST', '/')

    def get_pg_dsn(self):
        return f"host='{self.pg_host}' port='{self.pg_port}' dbname='{self.pg_dbname}' user='{self.pg_user}' password='{self.pg_pass}'"
    
    def get_rmq_dsn(self):
        return f"pyamqp://{self.rmq_user}:{self.rmq_pass}@{self.rmq_host}:{self.rmq_port}/{self.rmq_vhost}"


class __DevConfig(__BaseConfig):
    DEBUG = True
    ENV = 'development'
    LL = LogLevel.debug


class __ProdConfig(__BaseConfig):
    DEBUG = False
    ENV = 'production'
    LL = LogLevel.info


if os.environ.get('MODE') == 'prod':
    class Config(__ProdConfig):
        pass

else:
    class Config(__DevConfig):
        pass
