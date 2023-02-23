from pandemsource.suggestion import extract_span
import pandas as pd

def annotate(text, suggestion):
  espan = extract_span.ExtractSpan()
  #ix = [*range(0, len(text))]
  #df = extract.get_span_from_df(pd.DataFrame({"index":ix, "text":text, "label":[(1 if s == ["suggestion"] else 0) for s in suggestion]}))
  #df = extract.get_span_from_df(pd.DataFrame({"index":ix, "text":text, "label":[1 for s in suggestion]}))
  list_suggestion = espan.get_span_from_text_list([(t if s == ["suggestion"] else "") for t, s in zip(text, suggestion)])

  return list_suggestion
