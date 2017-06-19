# coding:utf-8
import os

mongo_config = {}
mongo_config['host'] = "localhost"
mongo_config['port'] = 27017
mongo_config['user'] = 'admin'
mongo_config["pwd"] = 'admin'

__mongo_host = os.environ.get('MONGO_HOST')
if __mongo_host:
    mongo_config['host'] = __mongo_host

__mongo_port = os.environ.get('MONGO_PORT')
if __mongo_port:
    mongo_config['port'] = __mongo_port

__mongo_user = os.environ.get('MONGO_USER')
if __mongo_user:
    mongo_config['user'] = __mongo_user

__mongo_pwd = os.environ.get("MONGO_PWD")
if __mongo_pwd:
    mongo_config['pwd'] = __mongo_pwd