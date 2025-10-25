import os
from datetime import datetime

from network_security.constants import training_pipeline as tp


class TrainingPipelineConfig:
    def __init__(self, timestamp=datetime.now()) -> None:
        timestamp = timestamp.strftime("%m_%d_%Y_%H_%M_%S")
        self.pipeline_name = tp.PIPELINE_NAME
        self.artifact_dir_name = tp.ARTIFACT_DIR
        self.artifact_dir = os.path.join(self.artifact_dir_name, timestamp)
        self.timestamp: str = timestamp


class DataIngestionConfig:
    def __init__(self, tp_config: TrainingPipelineConfig) -> None:
        self.data_ingestion_dir: str = os.path.join(
            tp_config.artifact_dir, tp.DATA_INGESTION_DIR_NAME
        )
        self.feature_store_file_path: str = os.path.join(
            self.data_ingestion_dir, tp.DATA_INGESTION_FEATURE_STORE_DIR, tp.FILE_NAME
        )
        self.training_file_path: str = os.path.join(
            self.data_ingestion_dir, tp.DATA_INGESTION_INGESTED_DIR, tp.TRAIN_FILE_NAME
        )
        self.testing_file_path: str = os.path.join(
            self.data_ingestion_dir, tp.DATA_INGESTION_INGESTED_DIR, tp.TEST_FILE_NAME
        )
        self.train_test_split_ratio: float = tp.DATA_INGESTION_TRAIN_TEST_SPLIT_RATIO
        self.collection_name: str = tp.DATA_INGESTION_COLLECTION_NAME
        self.database_name: str = tp.DATA_INGESTION_DATABASE_NAME


class DataValidationConfig:
    def __init__(self, tp_config: TrainingPipelineConfig) -> None:
        self.data_validation_dir: str = os.path.join(
            tp_config.artifact_dir, tp.DATA_VALIDATION_DIR_NAME
        )
        self.valid_data_dir: str = os.path.join(
            self.data_validation_dir, tp.DATA_VALIDATION_VALID_DIR
        )
        self.invalid_data_dir: str = os.path.join(
            self.data_validation_dir, tp.DATA_VALIDATION_INVALID_DIR
        )
        self.valid_train_file_path: str = os.path.join(
            self.valid_data_dir, tp.TRAIN_FILE_NAME
        )
        self.valid_test_file_path: str = os.path.join(
            self.valid_data_dir, tp.TEST_FILE_NAME
        )
        self.invalid_train_file_path: str = os.path.join(
            self.invalid_data_dir, tp.TRAIN_FILE_NAME
        )
        self.invalid_test_file_path: str = os.path.join(
            self.invalid_data_dir, tp.TEST_FILE_NAME
        )
        self.drift_report_file_path: str = os.path.join(
            self.data_validation_dir,
            tp.DATA_VALIDATION_DRIFT_REPORT_DIR,
            tp.DATA_VALIDATION_DRIFT_REPORT_FILE_NAME,
        )
