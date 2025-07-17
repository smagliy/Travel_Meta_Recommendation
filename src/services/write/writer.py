import os
import pandas as pd

from abc import ABC, abstractmethod

from src.services.log.logger import LoggerConfig

logging = LoggerConfig.set_up_logger()


class BaseWriter(ABC):
    @abstractmethod
    def write(self):
        pass


class WriterParquet(BaseWriter):
    def __init__(self, path: str, file_name: str, df: pd.DataFrame):
        self.full_path = os.path.join(path, file_name)
        self.df = df
        os.makedirs(os.path.dirname(self.full_path), exist_ok=True)

    def write(self):
        try:
            self.df.to_parquet(self.full_path, index=False)
            logging.info(f"Parquet file written successfully to: {self.full_path}")
        except Exception as e:
            logging.exception(f"Failed to write Parquet file: {self.full_path}; Exception: {e}")

