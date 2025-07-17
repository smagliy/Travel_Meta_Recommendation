import pandas as pd

from typing import List

from src.tasks.base_task import BaseTask
from src.services.aggregation.join import JoinService
from src.services.aggregation.calculation import CalculationService
from src.services.log.logger import LoggerConfig

logging = LoggerConfig.set_up_logger()


class CurrencyProcessor(BaseTask):
    def __init__(
        self,
        bids_df: pd.DataFrame,
        currency_df: pd.DataFrame,
        countries: List[str],
        bid_date_col: str = "BidDate",
        rate_date_col: str = "ValidFrom",
        rate_col: str = "ExchangeRate"
    ):
        self.bids_df = bids_df
        self.currency_df = currency_df[[rate_date_col, rate_col]]
        self.countries = countries
        self.bid_date_col = bid_date_col
        self.rate_date_col = rate_date_col
        self.rate_col = rate_col

    def _aggregate(self) -> pd.DataFrame:
        try:
            logging.info("Merging bids and currency data...")
            merged_df = JoinService.join_dataframes(
                self.bids_df,
                self.currency_df,
                self.bid_date_col,
                self.rate_date_col
            )
            logging.info(f"Merged DataFrame shape: {merged_df.shape}")

            for country in self.countries:
                logging.info(f"Converting currency for country: {country}")
                merged_df[country] = CalculationService.divide_columns(
                    merged_df,
                    country,
                    self.rate_col
                )
            return merged_df
        except Exception as e:
            logging.error(f"Error during currency aggregation: {e}", exc_info=True)
            raise

    def run(self, field_columns=None) -> pd.DataFrame:
        try:
            logging.info("Running currency conversion pipeline...")
            converted_df = self._aggregate().drop(self.currency_df.columns, axis=1)
            if field_columns:
                logging.info(f"Selecting final output columns: {field_columns}")
                return converted_df[field_columns]
            else:
                logging.info("Returning full converted DataFrame")
                return converted_df
        except Exception as e:
            logging.error(f"CurrencyProcessor pipeline failed: {e}", exc_info=True)
            raise

