import pandas as pd


def convert_grist_date_to_date(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for date_col in columns:
        df[date_col] = pd.to_datetime(df[date_col], unit="s").astype(
            dtype="datetime64[s]"
        )
    return df
