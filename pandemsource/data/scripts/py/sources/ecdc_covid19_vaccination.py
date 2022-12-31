import pandas as pd
import numpy as np
import re
from typing import Optional

INIT_AGES = [
    "1_age<60",
    "1_age60+",
    "age<18",
    "age0_4",
    "age10_14",
    "age15_17",
    "age18_24",
    "age25_49",
    "age5_9",
    "age50_59",
    "age60_69",
    "age70_79",
    "age80+"
]

AGES_MAP = {
    "1_age<60":"0-60",
    "1_age60+":"60+",
}

AGG_FUNC = {
    'FirstDose': 'sum',
    'SecondDose': 'sum',
    'DoseAdditional1': 'sum',
    'DoseAdditional2': 'sum',
    'DoseAdditional3': 'sum',
    'UnknownDose': 'sum',
    'Denominator': 'max',
    'Population': 'max'
}


def df_transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.groupby(["YearWeekISO", "Region", "TargetGroup"]).agg(AGG_FUNC).reset_index()
    df = split_target_group_col(df)
    column_names = ['FirstDose', 'SecondDose', 'DoseAdditional1', 'DoseAdditional2', 'DoseAdditional3', 'UnknownDose']
    df['TotalDosesInjected'] = df[column_names].sum(axis=1)
    df['AgeGroup'] = df['AgeGroup'].apply(normalize_age_groups)
    df['TargetPopulation'] = df['TargetPopulation'].str.lower()
    df["line_number"] = range(1, len(df)+1)
    return df


def split_target_group_col(df: pd.DataFrame) -> pd.DataFrame:
    df_all = df[df["TargetGroup"] == "ALL"].reset_index()
    df_population = df[df["TargetGroup"].str.lower().isin(["hcw","ltcf"])].reset_index()
    df_recommended_population = df_population.copy(deep=True)
    df_age_group = df[df["TargetGroup"].str.lower().isin(INIT_AGES)].reset_index()
    df_all.rename(columns={"TargetGroup": "AgeGroup"}, inplace=True)
    df_all.loc[:, "TargetPopulation"] = np.nan
    df_all.loc[:, "AgeGroup"] = np.nan
    df_recommended_population.rename(columns={"TargetGroup": "TargetPopulation"}, inplace=True)
    df_recommended_population.loc[:, "TargetPopulation"] = "recommend_population"
    df_recommended_population = df_recommended_population.groupby(["YearWeekISO", "Region", "TargetPopulation"]).agg(AGG_FUNC).reset_index()
    df_population.rename(columns={"TargetGroup": "TargetPopulation"}, inplace=True)
    df_age_group.rename(columns={"TargetGroup": "AgeGroup"}, inplace=True)
    return pd.concat([df_all, df_population, df_age_group, df_recommended_population])


def normalize_age_groups(age_group: str) -> Optional[str]:
    if pd.isnull(age_group):
        return np.nan
    elif age_group.lower() in AGES_MAP:
        return AGES_MAP[age_group.lower()]
    else:
        return age_group
