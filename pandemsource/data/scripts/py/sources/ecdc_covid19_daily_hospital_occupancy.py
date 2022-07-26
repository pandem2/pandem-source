def df_transform(df):
    df['country'] = df['country'].apply(str.upper)
    return df