import pandas as pd

from typing import List


class TransformService:
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

    @staticmethod
    def transpose_dataframe(df: pd.DataFrame, column_list: List[str],
                            var_name: str, value_name: str, value_vars=None):
        return df.melt(
            id_vars=column_list,
            value_vars=value_vars,
            var_name=var_name,
            value_name=value_name
        )

    def reshape_by_date_column(self, date_column: str, id_column: str, date_format: str,
                               var_name: str, value_name: str = 'BidValue'):
        self.df[date_column] = pd.to_datetime(self.df[date_column], errors='coerce', format=date_format)
        value_vars = self.df.columns.difference([id_column, date_column])
        return self.transpose_dataframe(self.df, [date_column], var_name,
                                        value_name, value_vars=value_vars.tolist())

    def group_and_count(self, group_columns: List[str], count_column_name: str = 'Count') -> pd.DataFrame:
        return (
            self.df.groupby(group_columns)
            .size()
            .reset_index(name=count_column_name)
            .sort_values(group_columns)
            .reset_index(drop=True)
        )



