import pandas as pd


class JoinService:
    @staticmethod
    def join_dataframes(left_df: pd.DataFrame, right_df: pd.DataFrame,
                        left_table_col: str, right_table_col: str):
        return pd.merge(
            left_df,
            right_df,
            left_on=left_table_col,
            right_on=right_table_col,
            how="left"
        )
