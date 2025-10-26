import sys

from network_security.components.data_ingestion import DataIngestion
from network_security.components.data_validation import DataValidation
from network_security.components.data_transformation import DataTransformation
from network_security.exception.exception import NetworkSecurityException
from network_security.logging.logger import logging
from network_security.entity.config import (
    DataIngestionConfig,
    DataValidationConfig,
    DataTransformationConfig,
    TrainingPipelineConfig,
)

if __name__ == "__main__":
    try:
        data_ingestion = DataIngestion(
            data_ingestion_config=DataIngestionConfig(
                tp_config=TrainingPipelineConfig()
            )
        )
        logging.info("Initiating data ingestion")
        data_ingestion_artifact = data_ingestion.initiate_data_ingestion()

        logging.info("Data ingestion completed")
        print(data_ingestion_artifact)

        data_validation = DataValidation(
            data_ingestion_artifact=data_ingestion_artifact,
            data_validation_config=DataValidationConfig(
                tp_config=TrainingPipelineConfig()
            ),
        )

        logging.info("Initiating data validation")
        data_validation_artifact = data_validation.initiate_data_validation()
        logging.info("Data validation completed")
        print(data_validation_artifact)
        data_transformation = DataTransformation(
            data_transformation_config=DataTransformationConfig(
                tp_config=TrainingPipelineConfig()
            ),
            data_validation_artifact=data_validation_artifact
        )
        logging.info("Initiating data transformation")
        data_transformation_artifact = data_transformation.initiate_data_transformation()
        print(data_transformation_artifact)
        logging.info("Data transformation completed")

    except Exception as e:
        raise NetworkSecurityException(e, sys)
