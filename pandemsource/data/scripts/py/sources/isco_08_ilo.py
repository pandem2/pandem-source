import pandas as pd

def df_transform(df: pd.DataFrame) -> pd.DataFrame:
    print(df.info())
    print(df.head())
    return df