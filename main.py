import sys

from network_security.components.data_ingestion import DataIngestion
from network_security.exception.exception import NetworkSecurityException
from network_security.logging.logger import logging
from network_security.entity.config import DataIngestionConfig, TrainingPipelineConfig

if __name__ == "__main__":
    try:
        data_ingestion = DataIngestion(
            data_ingestion_config=DataIngestionConfig(
                tp_config=TrainingPipelineConfig()
            )
        )
        logging.info("Initiating data ingestion")
        data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
        print(data_ingestion_artifact)
    except Exception as e:
        raise NetworkSecurityException(e, sys)