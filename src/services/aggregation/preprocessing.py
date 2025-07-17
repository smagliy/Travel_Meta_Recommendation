import pandas as pd


class PreprocessingService:
    @staticmethod
    def standardize_column_to_str(df: pd.DataFrame, id_column: str) -> pd.DataFrame:
        df[id_column] = df[id_column].astype(str)
        return df

    @staticmethod
    def standardize_column_to_numeric(df: pd.DataFrame, id_column: str) -> pd.DataFrame:
        df[id_column] = pd.to_numeric(df[id_column], errors="coerce")
        return df

    @staticmethod
    def standardize_column_to_datetime(df: pd.DataFrame, id_column: str,
                                       data_format: str) -> pd.DataFrame:
        df[id_column] = pd.to_datetime(
            df[id_column],
            errors='coerce'
        ).dt.strftime(data_format)
        return df
