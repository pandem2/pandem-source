from pandemsource.suggestion import extract_span
import pandas as pd

def annotate(text, suggestion):
  extract = extract_span.ExtractSpan()
  ix = [*range(0, len(text))]
  df = extract.get_span_from_df(pd.DataFrame({"index":ix, "text":text, "label":[(1 if s == ["suggestion"] else 0) for s in suggestion]}))
  #df = extract.get_span_from_df(pd.DataFrame({"index":ix, "text":text, "label":[1 for s in suggestion]}))
  #if len(df[pd.notna(df["Cleaned_stentence"])])>0:
  #  breakpoint()
  return df["Suggestion"].to_list()
