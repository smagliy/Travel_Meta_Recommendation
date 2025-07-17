import pandas as pd


class FilterService:
    @staticmethod
    def filter_by_prefix(
        df: pd.DataFrame,
        field_name: str,
        prefix: str,
        new_field_name: str = None,
        exclude: bool = False
    ) -> pd.DataFrame:
        df[field_name] = df[field_name].astype(str)
        condition = df[field_name].str.startswith(prefix)
        df_filtered = df[~condition] if exclude else df[condition]
        df_filtered = df_filtered.copy()
        if new_field_name:
            df_filtered.loc[:, new_field_name] = df_filtered[field_name]
        return df_filtered







