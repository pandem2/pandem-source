import pandas as pd

def df_transform(df):
    df = rebuild_columns(add_same_indicator(df))
    df['country'] = df['country'].apply(str.upper)
    df['line_number'] = range(1, len(df)+1) 
    return df

def add_same_indicator(df: pd.DataFrame) -> pd.DataFrame:
    """Adds values of same indicators due to several measure for a week"""
    return df.groupby(['indicator', 'year_week', 'country'])[['value']].sum().reset_index()

def rebuild_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Creates 4 new columns for each inficator and transforms daily in weekly values"""
    indicators = ['Daily hospital occupancy', 'Daily ICU occupancy', 'Weekly new hospital admissions per 100k', 'Weekly new ICU admissions per 100k']
    new_indicators = list(map(lambda x: x.lower().replace(' ', '_'), indicators))
    weekly_indicators = dict()
    # Initialize new columns
    for ind in new_indicators:
        df[ind] = pd.NA
    # Regroup indicators by (week, country)
    for row in df.index:
        indicator = df['indicator'][row].lower().replace(' ','_')
        value = df['value'][row]
        week = df['year_week'][row]
        country = df['country'][row]
        try:
            weekly_indicators[(week, country)].append({indicator: value})
        except KeyError:
            weekly_indicators[(week, country)] = [{indicator: value}]
    df = df.drop_duplicates(subset=['year_week', 'country'])
    # Build new columns
    for k, indicators_list in weekly_indicators.items():
        week, country = k[0], k[1]
        for indicator in indicators_list:
            for name, value in indicator.items():
                df.loc[(df['year_week'] == week) & (df['country'] == country), name] = value
    df = df.drop(columns=['value', 'indicator'])
    return df