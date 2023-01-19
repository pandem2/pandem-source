import numpy as np
import pandas as pd
from datetime import date, timedelta

INIT_DATE = date(2023, 1, 1)
DEU_POP = 84438383

def df_transform(df: pd.DataFrame) -> pd.DataFrame:
    normalize_column_names(df)
    normalize_country(df)
    normalize_age_groups(df)
    normalize_dates(df)
    df = split_interest_columns(df)
    generate_population_column(df)
    df = pd.concat([df.groupby(["date", "country"]).sum(numeric_only = True).reset_index(), df], ignore_index = True)
    df['line_number'] = range(1, len(df)+1)
    return df


def normalize_column_names(df: pd.DataFrame):
    df.columns = map(str.lower, df.columns)
    df.rename(columns={"time": "date"}, inplace=True)


def normalize_country(df: pd.DataFrame):
    df["country"] = [str.upper(country) for country in df["country"]]


def normalize_age_groups(df: pd.DataFrame):
    df["age"] = [do_normalize_age(age) for age in df["age"]]


def do_normalize_age(age_group: str) -> str:
    age_mapping = {"A": "<25", "B": "25-64", "C": ">64"}
    return age_mapping[age_group]


def normalize_dates(df: pd.DataFrame):
    df["date"] = [do_normalize_dates(date) for date in df["date"]]


def do_normalize_dates(days_since_init_date: str) -> str:
    days_since_init_date = int(days_since_init_date) - 1
    return str(INIT_DATE + timedelta(days=days_since_init_date))


def split_interest_columns(df: pd.DataFrame) -> pd.DataFrame:
    df["indicator"] = [str.lower(indicator) for indicator in df["indicator"]]
    df["cases"] = df.apply(lambda row: do_split_interest_columns(row, "cases", "synthetic_val"), axis=1)
    df["hospitalisations"] = df.apply(lambda row: do_split_interest_columns(row, "hospitalisations", "synthetic_val"), axis=1)
    df["deaths"] = df.apply(lambda row: do_split_interest_columns(row, "deaths", "synthetic_val"), axis=1)
    # Combine rows with same date but different indicators
    agg_func = {"cases": "sum", "hospitalisations": "sum", "deaths": "sum"}
    df = df.groupby(by=["date", "age", "country"]).aggregate(agg_func)
    return df.reset_index()


def do_split_interest_columns(row: pd.Series, indicator_name: str, base_col: str) -> pd.Series:
    return row[base_col] if row["indicator"] == indicator_name else np.nan

def generate_population_column(df: pd.DataFrame):
    df["population"] = DEU_POP
