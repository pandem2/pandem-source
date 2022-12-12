import pandas as pd
from isoweek import Week


def df_transform(df):
    df["week"] = pd.to_datetime(df["dateRep"], format = '%d/%m/%Y').apply(lambda d: Week(d.isocalendar()[0], d.isocalendar()[1]).monday().strftime('%Gw%V'))
    df = df.groupby(["week", "geoId"]).agg({"cases":"sum", "deaths":"sum", "popData2020":"last"}).reset_index()
    df['line_number'] = range(1, len(df)+1) 
    return df

