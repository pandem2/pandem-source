import pandas as pd
import re

def df_transform(df):
    column_names = ['FirstDose', 'SecondDose', 'DoseAdditional1', 'DoseAdditional2', 'UnknownDose']
    df['TotalDosesInjected'] = df[column_names].sum(axis=1)
    df['TargetGroup'] = df['TargetGroup'].apply(normalize_age_groups)
    return df


def normalize_age_groups(age_group):
    nb = sorted(re.findall('[0-9]+', age_group))
    nb_size = len(nb)
    if nb_size == 1:
        if '<' in age_group:
            return f'< {nb} yr'
        elif '+' in age_group:
            return f'{nb} +yr'
    elif nb_size == 2:
        return f'{nb[0]} - {nb[1]} yr'
    else:
        return pd.NA