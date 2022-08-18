import pandas as pd

def df_transform(df):
    df = rebuild_columns(add_same_indicator(df).reset_index())
    df['country'] = df['country'].apply(str.upper)
    df['line_number'] = range(1, len(df)+1) 
    df.to_csv('/home/william/GitHub/pandem-source/pandemsource/devtools/rebuilt_daily_hosp.csv')
    return df

def add_same_indicator(df: pd.DataFrame) -> pd.DataFrame:
    """Adds values of same indicators due to several measure for a week"""
    return df.groupby(['indicator', 'year_week', 'country'])[['value']].sum()

def rebuild_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = split_columns(df)
    df = df.drop(columns=['value', 'indicator'])
    return df

def split_columns(df: pd.DataFrame) -> pd.DataFrame:
    indicators = ['Daily hospital occupancy', 'Daily ICU occupancy', 'Weekly new hospital admissions per 100k', 'Weekly new ICU admissions per 100k']
    new_indicators = list(map(lambda x: x.lower().replace(' ', '_'), indicators))
    print(new_indicators)
    for ind in new_indicators:
        df[ind] = pd.NA
    for row in df.index:
        indicator = df['indicator'][row]
        value = df['value'][row]
        df[indicator.lower().replace(' ','_')][row] = value
    return df