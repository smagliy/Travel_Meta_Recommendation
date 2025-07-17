import pandas as pd

from abc import ABC, abstractmethod


class BaseReader(ABC):
    @abstractmethod
    def read(self, delimiter: str = ',') -> pd.DataFrame:
        pass


class ReaderTxtFile(BaseReader):
    def __init__(self, path: str, header: list):
        self.path = path
        self.header = header

    def read(self, delimiter: str = ',') -> pd.DataFrame:
        try:
            num_columns = len(self.header)
            return pd.read_csv(
                self.path,
                header=None,
                names=self.header,
                delimiter=delimiter,
                usecols=range(num_columns)
            )
        except Exception as e:
            raise RuntimeError(f"Failed to read file at {self.path}: {e}")







