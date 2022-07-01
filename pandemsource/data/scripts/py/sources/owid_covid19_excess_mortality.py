import pandas as pd
import time

# Disable the false positive "SettingWithCopyWarning"
pd.options.mode.chained_assignment = None

AGE_GROUPS = ['0-14', '15-64', '65-74', '75-84']

def df_transform(df: pd.DataFrame) -> pd.DataFrame:
  df = df[df.time_unit != 'monthly']
  excess_death_all_ages = build_excess_death_all_ages(df)
  excess_death_age_groups = build_excess_death_age_groups(df)
  final_df = pd.concat([excess_death_all_ages, excess_death_age_groups])
  final_df['line_number'] = range(1, len(final_df)+1) 
  return final_df


def build_excess_death_all_ages(df: pd.DataFrame) -> pd.DataFrame:
    df = df[['location', 'date', 'p_scores_all_ages']]
    df['date'] = df['date'].apply(get_iso_week)
    df['age_group'] = pd.NA
    df = df.rename(
        columns={
            'location': 'country',
            'date': 'iso_week',
            'p_scores_all_ages': 'excess_death'
        }
    )
    return df


def build_excess_death_age_groups(ref: pd.DataFrame) -> pd.DataFrame:
    ref = ref.rename(columns={'location': 'country'})
    df = ref[['country', 'date']]
    df['age_group'] = pd.NA
    df = add_age_groups_rows(df)
    df_age_groups = fill_age_groups_rows(df, ref)
    df_age_groups['date'] = df_age_groups['date'].apply(get_iso_week)
    df_age_groups = df_age_groups.rename(columns={'date': 'iso_week'})
    return df_age_groups


def add_age_groups_rows(df: pd.DataFrame) -> pd.DataFrame:
    new_rows = []
    for i in df.index:
        if pd.notna(df['country'][i]) and pd.notna(df['date'][i]):
            for j in range(4):
                row = [df['country'][i], df['date'][i], AGE_GROUPS[j], pd.NA]
                new_rows.append(row)
    age_groups_df = pd.DataFrame(
        new_rows, 
        columns=['country', 'date', 'age_group', 'excess_death']
    )
    return age_groups_df


def fill_age_groups_rows(df: pd.DataFrame, ref: pd.DataFrame) -> pd.DataFrame:
    for i in ref.index:
        for age in AGE_GROUPS:
            age_df = age
            age = age.replace('-', '_')
            if pd.notna(ref[f'p_scores_{age}'][i]):
                df.loc[
                    (df['country'] == ref['country'][i]) & (df['date'] == ref['date'][i]) & (df['age_group'] == age_df),
                    'excess_death'
                ] = ref[f'p_scores_{age}'][i]
    return df


def get_iso_week(date: str) -> str:
    if pd.notna(date):
        date_struct = time.strptime(date, '%Y-%m-%d')
        iso_week = time.strftime('%Y-W%U', date_struct)
    else:
        iso_week = pd.NA
    return iso_week
