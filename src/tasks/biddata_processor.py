import pandas as pd

from src.tasks.base_task import BaseTask
from src.services.aggregation.filter import FilterService
from src.services.aggregation.transform import TransformService
from src.services.aggregation.preprocessing import PreprocessingService
from src.services.log.logger import LoggerConfig

logging = LoggerConfig.set_up_logger()


class BidDataProcessor(BaseTask):
    def __init__(self, df: pd.DataFrame, date_column: str = "BidDate", id_column: str = "MotelID"):
        self.df = df
        self.date_column = date_column
        self.id_column = id_column
        self.var_name = "Country"
        self.value_name = "BidValue"

    def _aggregate(self, date_format: str, prefix: str = 'ERROR_') -> pd.DataFrame:
        try:
            logging.info("Starting data reshaping with TransformService...")
            transformer = TransformService(self.df)
            df_reshaped = transformer.reshape_by_date_column(
                date_column=self.date_column,
                id_column=self.id_column,
                date_format=date_format,
                var_name=self.var_name,
                value_name=self.value_name
            )
            logging.info(f"Reshaped data: {df_reshaped.shape[0]} rows")

            df_filtered = FilterService.filter_by_prefix(
                df=df_reshaped,
                field_name=self.value_name,
                prefix=prefix,
                new_field_name='ErrorType'
            )
            logging.info(f"Filtered data: {df_filtered.shape[0]} rows remaining after prefix filter")
            return df_filtered
        except Exception as e:
            logging.error(f"Error during aggregation: {e}", exc_info=True)
            raise

    def run(self, date_format: str = '%d-%m-%S-%Y') -> pd.DataFrame:
        logging.info("Running BidDataProcessor pipeline...")
        try:
            df_filtered = PreprocessingService.standardize_column_to_datetime(
                self._aggregate(date_format),
                self.date_column,
                date_format
            )

            logging.info("Grouping filtered data...")
            df_grouped = TransformService(df_filtered)\
                .group_and_count(
                group_columns=['ErrorType', self.date_column]
                )
            logging.info(f"Grouped result shape: {df_grouped.shape}")
            return df_grouped
        except Exception as e:
            logging.error(f"Pipeline failed: {e}", exc_info=True)
            raise

