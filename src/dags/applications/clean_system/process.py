import re
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd


def is_file_to_delete(s3_object_date, date_to_compare) -> bool:
    if s3_object_date < date_to_compare:
        return True
    return False


def check_date_format(s3_date: str) -> bool:
    if re.fullmatch(r"\d{8}", s3_date):
        try:
            datetime.strptime(s3_date, "%Y%m%d")
            return True
        except ValueError:
            print(f"{s3_date} is 8 digits but it doesn't respect AAAAMMDD format")
            return False
    return False


def check_hour_format(s3_hour: str) -> bool:
    if re.fullmatch(r"\d{2}h\d{2}", s3_hour):
        return True
    print(f"{s3_hour} doesn't respect HHhMM format (Example: 13h26)")
    return False


def safe_parse_date(date_str: str, is_date_format: bool) -> Optional[datetime]:
    if is_date_format:
        return datetime.strptime(date_str, "%Y%m%d")
    return None


def categorize_keys(df: pd.DataFrame) -> pd.DataFrame:
    """Process the list of S3 keys to identify which ones can be deleted."""

    # Process keys
    df["date_str"] = df["s3_keys"].str.split("/").str[-3]
    df["is_date_format"] = list(map(check_date_format, df["date_str"]))
    df["date"] = list(map(safe_parse_date, df["date_str"], df["is_date_format"]))
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month

    # Keep all rows exept those between the last 35 days
    cutoff_date = datetime.today() - timedelta(days=35)
    df_filtered = df[df["date"] <= cutoff_date].copy()

    # Step 2: Get the latest date per year-month
    max_dates = (
        df_filtered.dropna(subset=["date"])  # type: ignore
        .groupby(["year", "month"], as_index=False)["date"]
        .max()
        .rename(columns={"date": "max_month_date"})
    )  # type: ignore
    print(max_dates.head())

    # Step 3: Compare date to the max per group
    df_filtered = df_filtered.merge(max_dates, on=["year", "month"], how="left")
    df_filtered["is_latest_in_month"] = (
        df_filtered["date"] == df_filtered["max_month_date"]
    )

    return df_filtered
