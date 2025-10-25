import os
import sys
import dill
import pickle
import yaml
import numpy as np

from network_security.exception.exception import NetworkSecurityException
from network_security.logging.logger import logging

def read_yaml_file(file_path: str) -> dict:
    try:
        with open(file_path, 'rb') as yaml_file:
            return yaml.safe_load(yaml_file)
    except Exception as e:
        raise NetworkSecurityException(e, sys) from e
    

def write_yaml_file(file_path: str, data: object, replace: bool = False) -> None:
    try:
        if replace:
            if os.path.exists(file_path):
                os.remove(file_path)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file=file_path, mode='w') as yaml_file:
            yaml.dump(data=data, stream=yaml_file)
    except Exception as e:
        raise NetworkSecurityException(e, sys)