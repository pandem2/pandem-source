import numpy as np
import pandas as pd
from datetime import date, timedelta

INIT_DATE = date(2023, 10, 2)
max_indicators = ["people in hospital", "people in icu", "population"]
sum_indicators = ["cases", "hospitalisations", "deaths", "vaccination", "icu admissions"]
beds_capacity = ["number_of_icu_beds", "number_of_ward_beds"]
calc_indicators = ["number_of_beds", "people_in_ward"]

ward_capacity = {"DE":249500, "NL":17500}
icu_capacity = {"DE":13800, "NL":455}

def df_transform(df: pd.DataFrame) -> pd.DataFrame:
    # Generate the data for all ages
    df = pd.concat([df.groupby(["Time", "country", "indicator"]).aggregate({"synthetic_val":"sum", "population":"sum"}).reset_index(), df], ignore_index = True)
    df["Age"] = ["ALL" if pd.isna(age) else age for age in df["Age"]]
    normalize_column_names(df)
    normalize_dates(df)
    df = split_interest_columns(df)
    df = daily_to_weekly(df)
    df["age"] = [age if age != "ALL" else None for age in df["age"]]
    add_bed_capacity(df) 
    add_calculated(df) 
    df['line_number'] = range(1, len(df)+1)
    return df


def normalize_column_names(df: pd.DataFrame):
    df.columns = map(str.lower, df.columns)
    df.rename(columns={"time": "date"}, inplace=True)


def normalize_dates(df: pd.DataFrame):
    df["date"] = [do_normalize_dates(date) for date in df["date"]]


def do_normalize_dates(days_since_init_date: str) -> str:
    days_since_init_date = int(days_since_init_date) - 1
    return str(INIT_DATE + timedelta(days=days_since_init_date))


def split_interest_columns(df: pd.DataFrame) -> pd.DataFrame:
    indicators = [*max_indicators, *sum_indicators]
    df["indicator"] = [str.lower(indicator) for indicator in df["indicator"]]
    for ind in indicators:
      if ind != "population":
        df[ind.replace(" ", "_")] = get_interest_column(df, ind)
    # Combine rows with same date but different indicators
    df = (df.groupby(by=["date", "age", "country"])
      .aggregate({**{ind.replace(" ","_"):"sum" for ind in sum_indicators}, **{ind.replace(" ","_"):"max" for ind in max_indicators}})
      .reset_index()
    )
    return df

def get_interest_column(df, indicator) -> pd.Series:
  return df.apply(lambda row: do_split_interest_columns(row, indicator, "synthetic_val"), axis=1)

def do_split_interest_columns(row: pd.Series, indicator_name: str, base_col: str):
    return row[base_col] if row["indicator"] == indicator_name else np.nan

def daily_to_weekly(daily_data: pd.DataFrame) -> pd.DataFrame:
    daily_data['date'] = pd.to_datetime(daily_data['date'])
    daily_data = daily_data.set_index('date')
    weekly_data = (daily_data.groupby(['country', 'age', pd.Grouper(freq='W-MON')])
      .agg({**{ind.replace(" ","_"):"sum" for ind in sum_indicators}, **{ind.replace(" ","_"):"max" for ind in max_indicators}})
      .reset_index()
    )
    daily_data['period_type'] = 'date'
    daily_data.reset_index(inplace=True)
    weekly_data['period_type'] = 'isoweek'
    return pd.concat([weekly_data, daily_data], ignore_index=True)

def add_bed_capacity(df):
    df["number_of_icu_beds"] = df.apply(lambda r: icu_capacity.get(r["country"]) if pd.isna(r["age"]) else None, axis = 1)
    df["number_of_ward_beds"] = df.apply(lambda r: ward_capacity.get(r["country"]) if pd.isna(r["age"]) else None, axis = 1)
    return df


def add_calculated(df):
    df["number_of_beds"] = df["number_of_icu_beds"] + df["number_of_ward_beds"]
    df["people_in_ward"] = df["people_in_hospital"] - df["people_in_icu"]
    return df
