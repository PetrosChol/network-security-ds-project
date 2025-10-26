import os
import sys
import pandas as pd
from scipy.stats import ks_2samp

from network_security.entity.artifact import (
    DataIngestionArtifact,
    DataValidationArtifact,
)
from network_security.entity.config import DataValidationConfig
from network_security.exception.exception import NetworkSecurityException
from network_security.logging.logger import logging
from network_security.constants.training_pipeline import SCHEMA_FILE_PATH
from network_security.utils.main_utils.utils import read_yaml_file, write_yaml_file


class DataValidation:
    def __init__(
        self,
        data_ingestion_artifact: DataIngestionArtifact,
        data_validation_config: DataValidationConfig,
    ) -> None:

        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self._schema_config = read_yaml_file(SCHEMA_FILE_PATH)
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    @staticmethod
    def read_data(file_path: str) -> pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def validate_number_of_columns(self, dataframe: pd.DataFrame) -> bool:
        try:
            number_of_columns = len(self._schema_config["columns"])
            logging.info(f"Required number of columns: {number_of_columns}")
            logging.info(f"Dataframe has columns: {len(dataframe.columns)}")
            if len(dataframe.columns) == number_of_columns:
                return True
            return False
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def validate_numerical_columns(self, dataframe: pd.DataFrame) -> bool:
        try:
            numerical_columns = self._schema_config["numerical_columns"]
            dataframe_columns = dataframe.columns
            missing_numerical_columns = []
            for num_col in numerical_columns:
                if num_col not in dataframe_columns:
                    missing_numerical_columns.append(num_col)

            if len(missing_numerical_columns) > 0:
                logging.info(f"Missing numerical columns: {missing_numerical_columns}")
                return False
            return True
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def detect_data_drift(
        self, base_df: pd.DataFrame, current_df: pd.DataFrame, threshold: float = 0.05
    ) -> bool:
        try:
            status = True
            report = {}
            for col in base_df.columns:
                d1 = pd.to_numeric(base_df[col], errors="coerce").dropna()
                d2 = pd.to_numeric(current_df[col], errors="coerce").dropna()
                is_same_dist = ks_2samp(d1, d2)

                if is_same_dist.pvalue >= threshold:
                    is_found = False
                else:
                    is_found = True
                    status = False

                report.update(
                    {
                        col: {
                            "p_value": float(is_same_dist.pvalue),
                            "drift_status": is_found,
                        }
                    }
                )

            drift_report_file_path = self.data_validation_config.drift_report_file_path

            # Create directory if not exists
            dir_path = os.path.dirname(drift_report_file_path)
            os.makedirs(dir_path, exist_ok=True)
            write_yaml_file(file_path=drift_report_file_path, data=report)

            return status

        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def initiate_data_validation(self) -> DataValidationArtifact:
        try:
            train_file_path = self.data_ingestion_artifact.train_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path

            # Reading the train and test data
            train_df = DataValidation.read_data(train_file_path)
            test_df = DataValidation.read_data(test_file_path)
            # Validate number of columns
            train_columns_ok = self.validate_number_of_columns(dataframe=train_df)
            test_columns_ok = self.validate_number_of_columns(dataframe=test_df)

            # Validate numerical columns
            train_numerical_ok = self.validate_numerical_columns(dataframe=train_df)
            test_numerical_ok = self.validate_numerical_columns(dataframe=test_df)

            # Validate data drift (only meaningful if numerical columns exist)
            drift_ok = self.detect_data_drift(base_df=train_df, current_df=test_df)

            overall_status = all(
                [
                    train_columns_ok,
                    test_columns_ok,
                    train_numerical_ok,
                    test_numerical_ok,
                    drift_ok,
                ]
            )

            # Ensure directories exist and write files to valid or invalid paths depending on overall_status
            if overall_status:
                os.makedirs(
                    os.path.dirname(self.data_validation_config.valid_train_file_path),
                    exist_ok=True,
                )
                train_df.to_csv(
                    self.data_validation_config.valid_train_file_path,
                    index=False,
                    header=True,
                )
                test_df.to_csv(
                    self.data_validation_config.valid_test_file_path,
                    index=False,
                    header=True,
                )

                valid_train_path = self.data_validation_config.valid_train_file_path
                valid_test_path = self.data_validation_config.valid_test_file_path
                invalid_train_path = ""
                invalid_test_path = ""
            else:
                os.makedirs(
                    os.path.dirname(
                        self.data_validation_config.invalid_train_file_path
                    ),
                    exist_ok=True,
                )
                train_df.to_csv(
                    self.data_validation_config.invalid_train_file_path,
                    index=False,
                    header=True,
                )
                test_df.to_csv(
                    self.data_validation_config.invalid_test_file_path,
                    index=False,
                    header=True,
                )

                valid_train_path = ""
                valid_test_path = ""
                invalid_train_path = self.data_validation_config.invalid_train_file_path
                invalid_test_path = self.data_validation_config.invalid_test_file_path

            data_validation_artifact = DataValidationArtifact(
                validation_status=overall_status,
                valid_train_file_path=valid_train_path,
                valid_test_file_path=valid_test_path,
                invalid_train_file_path=invalid_train_path,
                invalid_test_file_path=invalid_test_path,
                drift_report_file_path=self.data_validation_config.drift_report_file_path,
            )
            return data_validation_artifact
        except Exception as e:
            raise NetworkSecurityException(e, sys)
