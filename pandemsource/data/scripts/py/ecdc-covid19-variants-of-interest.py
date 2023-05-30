import pandas as pd
df = pd.concat(pd.read_html("https://www.ecdc.europa.eu/en/covid-19/variants-concern"))
df.to_csv("variants.csv")

