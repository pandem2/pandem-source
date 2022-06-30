import pandas as pd
import time

AGE_GROUPS = ['0-14', '15-64', '65-74', '75-84']

def df_transform(df: pd.DataFrame) -> pd.DataFrame:
  df = df[df.time_unit != 'monthly']
  excess_death_all_ages = build_excess_death_all_ages(df)
  excess_death_age_groups = build_excess_death_age_groups(df)
  return pd.concat([excess_death_all_ages, excess_death_age_groups])
  
  filtered_df = df.groupby(['day', 'destination'])['callsign'].count()
  filtered_df = filtered_df.reset_index()
  filtered_df['line_number'] = range(1, len(filtered_df)+1) 
  return filtered_df.rename(columns={"callsign": "flights"})


def build_excess_death_all_ages(df: pd.DataFrame) -> pd.DataFrame:
    df = df[['location', 'date', 'excess_proj_all_ages']]
    df['date'] = df['date'].apply(get_iso_week)
    df['age_group'] = pd.NA
    df = df.rename(columns={'location': 'country'})
    df.to_csv('/home/william/pandem-test/excess_death_all_ages.csv')
    return df


def build_excess_death_age_groups(ref: pd.DataFrame) -> pd.DataFrame:
    df = ref[['location', 'date']]
    df['age_group'] = pd.NA
    df = df.rename(columns={'location': 'country'})
    df = add_age_groups_rows(df, ref)

    #TODO: Fill the new rows with the death_excess value for each age_group
    #for i in df.index:
    #df = np.where(df1['stream'] == 2, 10, pd.NA)
    
    df.to_csv('/home/william/pandem-test/excess_death_age_groups.csv')
    return df


def add_age_groups_rows(df: pd.DataFrame, ref: pd.DataFrame) -> pd.DataFrame:
    for i in ref.index:
        new_rows = []
        for j in range(4):
            row = {df['location'][i], df['date'][i], AGE_GROUPS[j]}
            new_rows.append(row)
        for row in new_rows:
            df.append(row)
    return df


def get_iso_week(date: str) -> str:
    date_struct = time.strptime(date, '%Y-%m-%d')
    return time.strftime('%U', date_struct)
