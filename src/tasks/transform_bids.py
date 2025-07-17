import pandas as pd

from typing import List, Tuple

from src.tasks.base_task import BaseTask
from src.services.aggregation.calculation import CalculationService
from src.services.aggregation.transform import TransformService
from src.services.aggregation.join import JoinService
from src.services.log.logger import LoggerConfig

logging = LoggerConfig.set_up_logger()


class BidTransformer(BaseTask):
    def __init__(self, bids_df: pd.DataFrame, exchange_df: pd.DataFrame, fields_list: List[str],
                 bid_date_col: str = "BidDate", rate_date_col: str = "ValidFrom", rate_col: str = "ExchangeRate"):
        self.bids_df = bids_df
        self.exchange_df = exchange_df
        self.fields_list = fields_list
        self.bid_date_col = bid_date_col
        self.rate_date_col = rate_date_col
        self.rate_col = rate_col
        self.new_field_name = "Price"
        self.country_field_name = "Country"

    def _aggregate(self, prefix: str = "ERROR_", extra_list_ids: Tuple[str] = ("MotelID", "BidDate")) -> pd.DataFrame:
        logging.info("Starting aggregation for bid transformation.")
        try:
            logging.debug(f"Filtering required columns: {extra_list_ids + tuple(self.fields_list)}")
            filtered_df = self.bids_df[list(extra_list_ids) + self.fields_list]

            logging.debug("Transforming dataframe using melt.")
            df_melted = TransformService.transpose_dataframe(
                filtered_df,
                list(extra_list_ids),
                var_name=self.country_field_name,
                value_name=self.new_field_name
            )

            logging.debug("Joining with exchange rate data.")
            df_joined = JoinService.join_dataframes(
                df_melted,
                self.exchange_df,
                self.bid_date_col,
                self.rate_date_col
            ).dropna(subset=[self.new_field_name, self.rate_col])

            logging.debug("Calculating converted prices.")
            df_joined[self.new_field_name] = CalculationService.divide_columns(
                df_joined, self.new_field_name, self.rate_col
            )

            logging.info("Aggregation completed successfully.")
            return df_joined

        except Exception as e:
            logging.exception("Aggregation failed due to an error.")
            raise

    def run(self, field_columns: List[str] = None) -> pd.DataFrame:
        logging.info("Running BidTransformer task.")
        df = self._aggregate().drop(self.exchange_df.columns, axis=1)
        if field_columns:
            logging.debug(f"Returning only selected columns: {field_columns}")
            return df[field_columns]
        logging.debug("Returning full dataframe.")
        return df

