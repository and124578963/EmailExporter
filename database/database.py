import os

from pymongo import MongoClient

from common.config_controller import Config


class MongoDatabase:
    def __init__(self):
        conf = Config()
        db_name = conf.data["database"]["db_name"]
        host = conf.data["database"]["host"]
        port = conf.data["database"]["port"]
        login = os.getenv("DB_LOGIN")
        passw = os.getenv("DB_PASSWRD")
        uri = f"mongodb://{login}:{passw}@{host}:{port}/"
        self.connect = MongoClient(uri)[db_name]

    def table(self, table_name):
        return self.connect[table_name]

