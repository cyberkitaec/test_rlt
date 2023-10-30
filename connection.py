from pymongo import MongoClient
from config import CONNECTION_URI

def get_database():
    client = MongoClient(CONNECTION_URI)

    return client['rlt']