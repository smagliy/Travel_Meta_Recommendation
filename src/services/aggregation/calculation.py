import pandas as pd


class CalculationService:
    @staticmethod
    def divide_columns(df: pd.DataFrame, numerator: str,
                       denominator: str, round_value: int = 3) -> pd.Series:
        return (pd.to_numeric(df[numerator], errors="coerce"
                                          ) / pd.to_numeric(df[denominator], errors="coerce")
                             ).round(round_value)
