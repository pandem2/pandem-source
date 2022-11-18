import pandas as pd
import re

def df_transform(df: pd.DataFrame) -> pd.DataFrame:
    df = fix_csv_tsv_mix_format_issues(df)
    df = df[(df['unit'].str.contains("NR|P_HTHAB", regex="True"))]
    df = df[df['facility'].str.contains("HBEDT_CUR")]
    df = df.drop(columns=["line_number", "facility"])
    df = df.melt(id_vars=["unit", "geo\\time", "file"], var_name="year", value_name="number_of_hospital_beds")
    df = remove_letters_in_numeric_columns(df)
    df = separate_nr_hthab(df)
    df = df[~df['geo\\time'].isin(['NO','RS','LI','IS','TR','ME','MK','CH','AL','FX'])]
    df = df.astype({"year": str})
    df["year"] = pd.to_datetime(df["year"], format="%Y")
    df["line_number"] = range(1, len(df)+1)
    return df

def fix_csv_tsv_mix_format_issues(df: pd.DataFrame) -> pd.DataFrame:
    # Remove one or more spaces with a single space and then leading and trailing white spaces
    df = df.applymap(lambda x: re.sub('\s+', ' ', x) if isinstance(x, str) else x)
    df = df.apply(lambda x: x.astype(str).str.strip() if isinstance(x, object) else x)
    df = df.replace([":", ": b", ": d"], pd.NA)
    df.columns = df.columns.str.replace(" ", "")
    return df


def remove_letters_in_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.astype({"number_of_hospital_beds": str})
    check_if_number_or_punctuation = lambda char: char.isdigit() or char in ["_","."]
    df["number_of_hospital_beds"] = df["number_of_hospital_beds"].apply(
        lambda x: ''.join(filter(check_if_number_or_punctuation, x))
    )
    df["number_of_hospital_beds"] = pd.to_numeric(df["number_of_hospital_beds"])
    return df


def separate_nr_hthab(df: pd.DataFrame) -> pd.DataFrame:
    hundred_k_rows = df[df['unit'] == "P_HTHAB"]
    data = []
    for index, row in hundred_k_rows.iterrows():
        number_of_hospital_beds = df[(df["geo\\time"] == row["geo\\time"]) & (df["year"] == row["year"]) & (df["unit"] == "NR")]["number_of_hospital_beds"].values[0]
        data.append([row["geo\\time"], row["year"], number_of_hospital_beds, row["number_of_hospital_beds"]])
        df = df.drop(index=index)
    new_df = pd.DataFrame(data, columns=["geo\\time", "year", "number_of_hospital_beds", "number_of_hospital_beds_per_100k"])
    return new_df