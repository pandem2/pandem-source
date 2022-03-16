import subprocess
import os
from . import worker


class ScriptExecutor(worker.Worker):
    def __init__(self, name, orchestrator_ref, settings): 
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings)
         
    def on_start(self):
        super().on_start()

    def script_path(self, script_type, script_name):
        return self.pandem_path("files", "scripts", script_type, f"{script_name}.{script_type}")

    def execute(self, script_name, script_type, working_dir):
        if not os.path.exists(working_dir):
            os.makedirs(working_dir)
        if script_type == "R":
            print(f'R script will be runned with: {self.script_path(script_type = script_type, script_name = script_name)}')
            subprocess.run(['Rscript', '--vanilla',  self.script_path(script_type = script_type, script_name = script_name)], cwd=working_dir) 
            print('R script has just been run')
        else: 
            raise NotImplementedError("So far only R scripts are supported")
