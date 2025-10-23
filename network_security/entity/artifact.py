from pydantic import BaseModel

class DataIngestionArtifact(BaseModel):
    train_file_path: str
    test_file_path: str