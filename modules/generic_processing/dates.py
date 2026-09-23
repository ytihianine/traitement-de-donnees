import logging

import pandas as pd


def _infer_epoch_unit(series: pd.Series) -> str | None:
    numeric_values = pd.to_numeric(series, errors="coerce").dropna()
    if numeric_values.empty:
        return None

    max_abs_value = abs(float(numeric_values.max()))
    if max_abs_value >= 1e14:
        return "us"
    if max_abs_value >= 1e11:
        return "ms"
    return "s"


def convert_grist_date_to_date(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for date_col in columns:
        logging.info(msg=f"Converting column {date_col} to datetime")

        series = df[date_col]
        numeric_series = pd.to_numeric(series, errors="coerce")
        valid_numeric_mask = numeric_series.notna()
        inferred_unit = _infer_epoch_unit(series)

        converted = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")

        if inferred_unit is not None:
            converted.loc[valid_numeric_mask] = pd.to_datetime(
                numeric_series[valid_numeric_mask],
                unit=inferred_unit,
                errors="coerce",
            )

        if (~valid_numeric_mask).any():
            converted.loc[~valid_numeric_mask] = pd.to_datetime(
                series[~valid_numeric_mask],
                errors="coerce",
            )

        df[date_col] = converted.astype(dtype="datetime64[s]")

    return df
