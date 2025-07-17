import pandas as pd

from typing import List

from src.tasks.base_task import BaseTask
from src.tasks.load_motels import LoadMotels
from src.services.aggregation.transform import TransformService
from src.services.aggregation.preprocessing import PreprocessingService
from src.services.log.logger import LoggerConfig

logging = LoggerConfig.set_up_logger()


class MaxMotelsProcessor(BaseTask):
    def __init__(self, df_motels: pd.DataFrame, df_bids: pd.DataFrame, field_name: str, country_cols: List[str]):
        self.df_motels = df_motels
        self.df_bids = df_bids
        self.field_name = field_name
        self.id_vars = ["MotelID", "MotelName", "BidDate"]
        self.countries = country_cols
        self.value_name = "Price"
        self.country_field_name = "Country"


    def _aggregate(self) -> pd.DataFrame:
        try:
            logging.info("Starting aggregation for MaxMotelsProcessor...")
            df = LoadMotels(self.df_motels, self.df_bids, self.field_name).run()

            logging.info(f"Joined DataFrame shape: {df.shape}")
            df_melted = TransformService.transpose_dataframe(
                df, self.id_vars, self.country_field_name,
                self.value_name, value_vars=self.countries
            )
            logging.info(f"DataFrame after melting: {df_melted.shape}")

            df_melted = PreprocessingService.standardize_column_to_numeric(df_melted, self.value_name)
            df_melted = df_melted.dropna(subset=[self.value_name])
            logging.info(f"DataFrame after numeric conversion and dropping NaNs: {df_melted.shape}")

            result = df_melted.loc[
                df_melted.groupby(self.id_vars)[self.value_name].idxmax()
            ].reset_index(drop=True)
            logging.info(f"Final result after groupby max: {result.shape}")
            return result
        except Exception as e:
            logging.error(f"Error in MaxMotelsProcessor._aggregate: {e}", exc_info=True)
            raise

    def run(self, field_columns=None) -> pd.DataFrame:
        try:
            logging.info("Running MaxMotelsProcessor...")
            df = self._aggregate()
            if field_columns:
                logging.info(f"Returning selected columns: {field_columns}")
                return df[field_columns]
            else:
                logging.info("Returning full result.")
                return df
        except Exception as e:
            logging.error(f"MaxMotelsProcessor run failed: {e}", exc_info=True)
            raise
