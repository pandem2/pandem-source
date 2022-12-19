import pandas as pd
import numpy as np
import re

GEO = "geo\\time"

def df_transform(df: pd.DataFrame) -> pd.DataFrame:
    print("--------------------------------------------------------hola")
    print(df)
    df = fix_csv_tsv_mix_format_issues(df)
    df = df[(df['unit'].str.contains("NR|P_HTHAB", regex="True"))]
    df = df.drop(columns=["line_number"])
    df = df.melt(id_vars=["unit", GEO, "facility", "file"], var_name="year", value_name="number_of_hospital_beds")
    df = remove_letters_in_numeric_columns(df)
    df = separate_nr_hthab(df)
    df = df[~df['geo\\time'].isin(['NO','RS','LI','IS','TR','ME','MK','CH','AL','FX'])]
    df = build_columns_of_interest(df)
    df = df.astype({"year": str})
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df["year"] = pd.to_datetime(df["year"], format="%Y")
    df["line_number"] = range(1, len(df)+1)
    print(df)
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
    data = []
    keys = dict() 
    for index, row in df.iterrows():
        key = (row[GEO],  row["facility"], row["year"])
        number_of_hospital_beds = row["number_of_hospital_beds"] if row["unit"] == "NR" else None
        number_of_hospital_beds_per_100k = row["number_of_hospital_beds"] if row["unit"] == "P_HTHAB" else None
        if key not in keys:
          keys[key] = [row[GEO],  row["facility"], row["year"], number_of_hospital_beds, number_of_hospital_beds_per_100k]
        else:
          keys[key] = [
            row[GEO],  
            row["facility"], 
            row["year"], 
            number_of_hospital_beds if number_of_hospital_beds is not None else keys[key][3], 
            number_of_hospital_beds_per_100k if number_of_hospital_beds_per_100k is not None else keys[key][4]
          ]
        
    new_df = pd.DataFrame([*keys.values()], columns=[GEO, "facility", "year", "number_of_hospital_beds", "number_of_hospital_beds_per_100k"])
    print(new_df)
    return new_df


def build_columns_of_interest(df: pd.DataFrame) -> pd.DataFrame:
    df["number_of_icu_operable_beds"] = df.apply(lambda x: build_column(x, "HBEDT_CUR", "number_of_hospital_beds"), axis=1)
    df["number_of_icu_operable_beds_per_100k"] = df.apply(lambda x: build_column(x, "HBEDT_CUR", "number_of_hospital_beds_per_100k"), axis=1)
    
    df["number_of_operable_beds"] = df.apply(lambda x: build_column(x, "HBEDT", "number_of_hospital_beds"), axis=1)
    df["number_of_operable_beds_per_100k"] = df.apply(lambda x: build_column(x, "HBEDT", "number_of_hospital_beds_per_100k"), axis=1)
    
    df["number_of_lctf_beds"] = df.apply(lambda x: build_column(x, "HBEDT_LT", "number_of_hospital_beds"), axis=1)
    df["number_of_lctf_beds_per_100k"] = df.apply(lambda x: build_column(x, "HBEDT_LT", "number_of_hospital_beds_per_100k"), axis=1)
    
    df["number_of_psy_beds"] = df.apply(lambda x: build_column(x, "HBEDI_PSY", "number_of_hospital_beds"), axis=1)
    df["number_of_psy_beds_per_100k"] = df.apply(lambda x: build_column(x, "HBEDI_PSY", "number_of_hospital_beds_per_100k"), axis=1)

    df["number_of_oth_beds"] = df.apply(lambda x: build_column(x, "HBEDT_OTH", "number_of_hospital_beds"), axis=1)
    df["number_of_oth_beds_per_100k"] = df.apply(lambda x: build_column(x, "HBEDT_OTH", "number_of_hospital_beds_per_100k"), axis=1)

    df["number_of_reh_beds"] = df.apply(lambda x: build_column(x, "HBEDT_REH", "number_of_hospital_beds"), axis=1)
    df["number_of_reh_beds_per_100k"] = df.apply(lambda x: build_column(x, "HBEDT_REH", "number_of_hospital_beds_per_100k"), axis=1)

    columns_to_sum = ["number_of_psy_beds", "number_of_oth_beds","number_of_reh_beds", "number_of_lctf_beds"]
    columns_100k_to_sum = ["number_of_psy_beds_per_100k", "number_of_oth_beds_per_100k","number_of_reh_beds_per_100k", "number_of_lctf_beds_per_100k"]

    df["number_of_non_icu_beds"] = df[columns_to_sum].sum(axis=1)
    df["number_of_non_icu_beds_per_100k"] = df[columns_100k_to_sum].sum(axis=1)
    
    df.drop(columns=
        [
            "number_of_hospital_beds",
            "number_of_hospital_beds_per_100k",
            "facility", 
            "number_of_lctf_beds",
            "number_of_psy_beds",
            "number_of_oth_beds",
            "number_of_reh_beds"
        ], inplace=True)
    df["number_of_non_icu_beds"] = df["number_of_non_icu_beds"].apply(lambda x: x if x != 0 else np.nan)
    df["number_of_non_icu_beds_per_100k"] = df["number_of_non_icu_beds_per_100k"].apply(lambda x: x if x != 0 else np.nan)
    return df


def build_column(row: pd.Series, facility: str, base_column: str) -> pd.DataFrame:
    return row[base_column] if row["facility"] == facility else None
