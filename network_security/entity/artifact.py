from pydantic import BaseModel


class DataIngestionArtifact(BaseModel):
    train_file_path: str
    test_file_path: str


class DataValidationArtifact(BaseModel):
    validation_status: bool
    valid_train_file_path: str
    valid_test_file_path: str
    invalid_train_file_path: str
    invalid_test_file_path: str
    drift_report_file_path: str


class DataTransformationArtifact(BaseModel):
    transformed_object_file_path: str
    transformed_train_file_path: str
    transformed_test_file_path: str
