import os
import sys
import json
import certifi
import pandas as pd
from dotenv import load_dotenv

from pymongo.mongo_client import MongoClient

from network_security.exception.exception import NetworkSecurityException
from network_security.logging.logger import logging

load_dotenv()

uri = os.getenv("MONGO_DB_URI")
ca = certifi.where()

class NetworkDataExtractor:
    def __init__(self) -> None:
        try:
            pass
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def convert_csv_to_json(self, file_path: str):
        try:
            data = pd.read_csv(file_path)
            data.reset_index(drop=True, inplace=True)
            records = list(json.loads(data.T.to_json()).values())

            return records
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def insert_data_to_mongodb(self, records: list, db_name: str, collection_name: str):
        try:
            self.db_name = db_name
            self.collection_name = collection_name
            self.records = records
            self.client = MongoClient(uri)

            self.db_name = self.client[self.db_name]
            self.collection_name = self.db_name[self.collection_name]
            self.collection_name.insert_many(self.records)

            return len(self.records)
        except Exception as e:
            raise NetworkSecurityException(e, sys)
        

if __name__ == "__main__":
    FILE_PATH = os.path.join("network_data", "phisingData.csv")
    DB_NAME = "NetworkSecurity"
    COLLECTION_NAME = "PhishingData"
    networkobject = NetworkDataExtractor()
    records = networkobject.convert_csv_to_json(FILE_PATH)
    if records is not None:
        number_of_records = networkobject.insert_data_to_mongodb(records, DB_NAME, COLLECTION_NAME)
        print(f"Number of records inserted to MongoDB: {number_of_records}")
