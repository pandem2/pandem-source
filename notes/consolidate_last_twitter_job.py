import pickle
import os
import json

jobs_path = os.path.join(os.environ.get("PANDEM_HOME"), "database/jobs.pickle")
with open(jobs_path, "rb") as fp:
  jobs = pickle.load(fp)

job_id = jobs[jobs.source == "twitter-covid19"].index.max()
print(f"taking job {job_id}")

current_files = jobs.source_files[job_id]
f = files[0] 
base_path = "/".join(f.split("/")[:-1])
allfiles = os.path.join(base_path, "allfiles.json")

# recovering all files from previous execution if already modified
if os.path.exists(allfiles):
  with open(allfiles, "r") as fp:
    files = json.load(fp)
else:
  with open(allfiles, "w") as fp:
    json.dump(files, fp)


dates = sorted({f.split(".")[0].split("_")[-2] for f in files})
chunks = sorted({f.split(".")[0].split("_")[-1] for f in files})

parts = f.split(".")[0].split("_")
parts[-1] = "CHUNK"
parts[-2] = "DATE"
base_file = f"{'_'.join(parts)}.json"

def get_file(base_file, date, chunk):
  return base_file.replace("CHUNK", chunk).replace("DATE", date)

def get_dest_file(base_file, date):
  return base_file.replace("CHUNK", "ALL").replace("DATE", date)

start_date = "2020-07-01"
end_date = "2020-12-31"
to_process = [d for d in dates if d >= start_date and d <= end_date]
new_files = []
for d in to_process:
  print(d)
  tweets = []
  for c in chunks:
    source = get_file(base_file, d, c)
    if os.path.exists(source):
      with open(source, "r") as fp:
        tweets.extend(json.load(fp))
  dest = get_dest_file(base_file, d)
  with open(dest, "w") as fp:
    json.dump(tweets, fp)
  new_files.append(dest)

jobs.at[job_id, "source_files"] = new_files
jobs.at[job_id, "file_sizes"] = [1 for f in new_files]
with open(jobs_path, "wb") as fp:
  pickle.dump(jobs, fp)
