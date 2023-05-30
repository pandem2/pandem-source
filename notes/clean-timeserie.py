import pickle
import os

ts_path = os.path.join(os.environ["PANDEM_HOME"],"files/variables/time_series.pi" ) 
var_path = os.path.join(os.environ["PANDEM_HOME"],"files/variables" ) 

with open(ts_path, "rb") as f:
  ts = pickle.load(f)

ind_to_delete = ["available_staff", "available_staff_per_100k", "available_staff_variation"]#"beds_occupancy_ratio", "icu_occupancy_ratio", "hospitalised_infected_patients", "number_of_operable_beds", "hospitalised_infected_patients_in_icu"]
source_to_delete = []#["twitter-covid19"]
i = 0
for k in [t for t in ts.keys() if (
        (len(ind_to_delete) == 0 or len([k for k, v in t if k == "indicator" and v in ind_to_delete]) > 0 ) and
        (len(source_to_delete) == 0 or len([k for k, v in t if k == "source" and v in source_to_delete]) > 0 )
      )
    ]:
  ts.pop(k)
  i = i + 1

 

with open(ts_path, "wb") as f:
  pickle.dump(ts, f)

print(f"{i} timeseries deleted")

j = 0
if len(source_to_delete) > 0:
  for root, dirs, files in  os.walk(var_path):
     for name in files:
       for s in source_to_delete:
         if s in name:
           p = os.path.join(root, name)
           if os.path.exists(p):
             os.remove(p)
             j = j + 1

print(f"{j} files deleted")

