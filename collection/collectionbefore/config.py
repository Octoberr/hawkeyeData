# coding:utf-8
import os

mongo_config = {}
mongo_config['host'] = "localhost"
mongo_config['port'] = 27017

__mongo_host = os.environ.get('MONGO_HOST')
if __mongo_host:
    mongo_config['host'] = __mongo_host

__mongo_port = os.environ.get('MONGO_PORT')
if __mongo_port:
    mongo_config['port'] = __mongo_port