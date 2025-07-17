import pandas as pd

from src.tasks.base_task import BaseTask
from src.services.aggregation.join import JoinService
from src.services.aggregation.preprocessing import PreprocessingService
from src.services.log.logger import LoggerConfig

logging = LoggerConfig.set_up_logger()


class LoadMotels(BaseTask):
    def __init__(self, df_motels: pd.DataFrame, df_bids: pd.DataFrame, field_name: str):
        self.df_motels = df_motels
        self.df_bids = df_bids
        self.field_name = field_name

    def _aggregate(self) -> pd.DataFrame:
        try:
            logging.info(f"Standardizing column '{self.field_name}' in both dataframes...")
            df_motels_std = PreprocessingService.standardize_column_to_str(self.df_motels, self.field_name)
            df_bids_std = PreprocessingService.standardize_column_to_str(self.df_bids, self.field_name)

            logging.info(f"Joining dataframes on field '{self.field_name}'...")
            df_joined = JoinService.join_dataframes(df_motels_std, df_bids_std,
                                                    self.field_name, self.field_name)

            logging.info(f"Join result shape...")
            return df_joined
        except Exception as e:
            logging.error(f"Error during joining motels and bids: {e}", exc_info=True)
            raise

    def run(self, field_columns=None) -> pd.DataFrame:
        try:
            logging.info("Running LoadMotels pipeline...")
            df = self._aggregate()

            if field_columns:
                logging.info(f"Filtering columns: {field_columns}")
                return df[field_columns]
            else:
                logging.info("Returning full joined DataFrame.")
                return df
        except Exception as e:
            logging.error(f"LoadMotels pipeline failed: {e}", exc_info=True)
            raise
