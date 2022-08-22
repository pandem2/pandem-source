import pandas as pd

def df_transform(df):
    column_names = ['FirstDose', 'SecondDose', 'DoseAdditional1', 'DoseAdditional2', 'UnknownDose']
    df['TotalDosesInjected'] = df[column_names].sum(axis=1)
    return df