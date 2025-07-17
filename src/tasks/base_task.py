import pandas as pd
from abc import ABC, abstractmethod


class BaseTask(ABC):
    @abstractmethod
    def _aggregate(self, *args):
        pass

    @abstractmethod
    def run(self, field_columns=None) -> pd.DataFrame:
        pass

