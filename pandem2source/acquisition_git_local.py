import os
import subprocess
from . import acquisition_git


class AcquisitionGITLocal(acquisition_git.AcquisitionGIT):
    def __init__(self, name, orchestrator_ref, settings): 
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings)
    
    def on_start(self):
        super().on_start()
        self._script_executor_proxy = self._orchestrator_proxy.get_actor('script_executor').get().proxy()

    def new_files(self, dls, last_hash):
        script_name = dls["acquisition"]["channel"]["changed_by"]["script_name"]
        script_type = dls["acquisition"]["channel"]["changed_by"]["script_type"]
        working_dir = self.source_path(dls)
        # executing the updating script
        self._script_executor_proxy.execute(script_name = script_name, script_type = script_type, working_dir = working_dir).get()

        # creating repo if it does no exists
        # if not os.path.exists(working_dir):
        #     os.makedirs(working_dir)
        res = subprocess.run(['git', 'status'], cwd = working_dir)
        if res.returncode != 0 :
            res = subprocess.run(["git", "init"], cwd = working_dir)
            res = subprocess.run(["git", "config", "user.name", "pandem2monitor"], cwd = working_dir)
            res = subprocess.run(["git", "config", "user.email", "pandem2monitor@pandem2.eu"], cwd = working_dir)
            res = subprocess.run(["git", "add", "."], cwd = working_dir)
            res = subprocess.run(["git", "commit", "-m" "Automatically initialized repo"], cwd = working_dir, capture_output=True)
        else :
            res = subprocess.run(["git", "add", "."], cwd = working_dir)
            res = subprocess.run(["git", "commit", "-m" "Committed new files"], cwd = working_dir, capture_output=True)
        
        return super().new_files(dls, last_hash)
