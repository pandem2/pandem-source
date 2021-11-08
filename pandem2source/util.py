import os

def check_pandem_home():
  if os.environ.get("PANDEM_HOME") is None:
    raise RuntimeError("The variable PANDEM_HOME needs to be set to a local folder in order to run pandem source")

