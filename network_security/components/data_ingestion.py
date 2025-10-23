import os
import sys
import pymongo
from typing import List
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split

from network_security.exception.exception import NetworkSecurityException
from network_security.logging.logger import logging
from network_security.entity.config import DataIngestionConfig
from network_security.entity.artifact import DataIngestionArtifact

from dotenv import load_dotenv
load_dotenv()

MONGO_DB_URI = os.getenv("MONGO_DB_URI")

class DataIngestion:
    def __init__(self, data_ingestion_config: DataIngestionConfig) -> None:
        try:
            self.data_ingestion_config = data_ingestion_config
        except Exception as e:
            raise NetworkSecurityException(e, sys)
        
    def export_collection_as_df(self) -> pd.DataFrame:
        """Export collection data as pandas DataFrame"""
        try:
            db_name = self.data_ingestion_config.database_name
            collection_name = self.data_ingestion_config.collection_name
            self.mongo_client = pymongo.MongoClient(MONGO_DB_URI)
            collection = self.mongo_client[db_name][collection_name]

            df = pd.DataFrame(list(collection.find()))
            if "_id" in df.columns.to_list():
                df = df.drop(columns=["_id"], axis=1)

            df.replace({"na":np.nan}, inplace=True)
            return df

        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def export_data_to_feature_store(self, df: pd.DataFrame) -> pd.DataFrame:
        """Export DataFrame to feature store as csv file"""
        try:
            feature_store_file_path = self.data_ingestion_config.feature_store_file_path
            dir_path = os.path.dirname(feature_store_file_path)
            os.makedirs(dir_path, exist_ok=True)
            df.to_csv(feature_store_file_path, index=False, header=True)
            
            return df
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def split_data_as_train_test(self, df: pd.DataFrame) -> None:
        try:
            train_set, test_set = train_test_split(
                df, test_size=self.data_ingestion_config.train_test_split_ratio, random_state=42
            )
            logging.info("Performed train test split")
            logging.info(
                "Exited split_data_as_train_test method of DataIngestion class"
            )
            dir_path = os.path.dirname(
                self.data_ingestion_config.training_file_path
            )
            os.makedirs(dir_path, exist_ok=True)
            logging.info(f"Exporting train/test file path")

            train_set.to_csv(
                self.data_ingestion_config.training_file_path, index=False, header=True
            )
            test_set.to_csv(
                self.data_ingestion_config.testing_file_path, index=False, header=True
            )

            logging.info(f"Exported train/test file path")


        except Exception as e:
            raise NetworkSecurityException(e, sys)



    def initiate_data_ingestion(self) -> DataIngestionArtifact:
        try:
            dataframe = self.export_collection_as_df()
            dataframe = self.export_data_to_feature_store(dataframe)
            self.split_data_as_train_test(dataframe)
            data_ingestion_artifact = DataIngestionArtifact(
                train_file_path=self.data_ingestion_config.training_file_path,
                test_file_path=self.data_ingestion_config.testing_file_path
            )
            return data_ingestion_artifact
        except Exception as e:
            raise NetworkSecurityException(e, sys)