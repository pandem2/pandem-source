import os
import subprocess
from . import acquisition


class AcquisitionGIT(acquisition.Acquisition):
    def __init__(self, name, orchestrator_ref, settings): 
        super().__init__(name = name, orchestrator_ref = orchestrator_ref, settings = settings, channel = "git")

    def new_files(self, dls, last_hash):
        if("url" in dls['acquisition']['channel']):
          repo_name = dls['acquisition']['channel']['url'].split('/')[-1].split(".")[0]
          repo_dir = self.source_path(dls, repo_name)
        else :
          repo_name = ""
          repo_dir = self.source_path(dls)

        source_dir = self.source_path(dls)
        if "branch" in dls['acquisition']['channel']:
            dist_branch = dls['acquisition']['channel']['branch']
        else :
            dist_branch = "master"

        # If the repository does not exists or if no commit is provided all files will be sent to the pipeline
        files_to_pipeline = []
        if not os.path.exists(repo_dir) or last_hash == "":
            # clonning repo is a repo remote url has been provided
            if("url" in dls['acquisition']['channel']):
              subprocess.run(['git', 'clone',  dls['acquisition']['channel']['url']], cwd=source_dir) 

            #send all repo files in target subdirectories to the pipeline actor
            if "paths" in dls['acquisition']['channel']:
                for subdir in dls['acquisition']['channel']['paths']:
                    files_paths = self._storage_proxy.list_files(self.source_path(dls, repo_name, subdir)).get()
                    files_to_pipeline.extend([file_path['path'] for file_path in files_paths ])
            else: 
              file_paths = self._storage_proxy.list_files(self.source_path(dls, repo_name)).get()
              files_to_pipeline.extend([file_path['path'] for file_path in files_paths ])

        # the repo already exists and we know the last commit 
        else :             
            subprocess.run(['git', 'pull',  'origin', dist_branch], cwd = repo_dir)
            
            #get updated files
            for subdir in dls['acquisition']['channel']['paths']:
                new_files_subdir = subprocess.run(['git', 'diff', '--name-only', last_hash, 'HEAD', subdir], 
                                            capture_output=True,
                                            text=True,
                                            cwd=repo_dir
                )
                if new_files_subdir.stdout != '':
                    #print(f'new files: {new_files.stdout}')
                    new_files = new_files_subdir.stdout.rstrip().split('\n')
                    files_paths =  [self.source_path(dls, repo_name, new_file) for new_file in new_files]
                    files_to_pipeline.extend(files_paths)
            
        new_commit = subprocess.run(['git', 'rev-parse', 'origin/'+dist_branch], 
                        capture_output=True,
                        text=True,
                        cwd=repo_dir 
        )                 
        return {"hash":last_commit, files:files_to_pipeline}           
