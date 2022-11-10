import pandas as pd
import re

def df_transform(df: pd.DataFrame) -> pd.DataFrame:
    df = fix_csv_tsv_mix_format_issues(df)
    df = df.melt(id_vars=["unit", "isco08", "geo\\time"], var_name="year", value_name="number_of_hospital_staff")
    df.reset_index(drop=True)
    df['line_number'] = range(1, len(df)+1) 
    return df

def fix_csv_tsv_mix_format_issues(df: pd.DataFrame) -> pd.DataFrame:
    # Remove one or more spaces with a single space and then leading and trailing white spaces
    df = df.applymap(lambda x: re.sub('\s+', ' ', x) if isinstance(x, str) else x)
    df = df.apply(lambda x: x.astype(str).str.strip() if isinstance(x, object) else x)
    df = df.replace([":", ": b", ": d"], pd.NA)
    df = df.dropna(subset= df.columns[3:], how='all')
    df.columns = df.columns.str.replace(" ", "")
    return df