import logging
import shutil
import tempfile
import os
import zipfile
import sys
import pins
import fsspec
from guild import remote as remotelib
from guild import var
from guild import remote_util

from . import meta_sync

log = logging.getLogger("guild.remotes.pins")

RUNS_PATH = ["runs"]
DELETED_RUNS_PATH = ["trash", "runs"]

class PinsRemoteType(remotelib.RemoteType):
    def __init__(self, _ep):
        pass

    def remote_for_config(self, name, config):
        return PinsRemote(name, config)

    def remote_for_spec (self, spec):
        pass

class PinsRemote (meta_sync.MetaSyncRemote):
    def __init__(self, name, config):
        self.name = name
        self.subdir = config.get("subdir", "/")
        fs_config = config['config']
        self.fs = fsspec.filesystem(**fs_config)

        self.local_env = remote_util.init_env(config.get("local-env"))
        self.local_sync_dir = meta_sync.local_meta_dir(name, str(fs_config))
        runs_dir = os.path.join(self.local_sync_dir, *RUNS_PATH)
        deleted_runs_dir = os.path.join(self.local_sync_dir, *DELETED_RUNS_PATH)
        super().__init__(runs_dir, deleted_runs_dir)

    def status(self, verbose=False):
        sys.stdout.write(f"{self.name} (Pins board {str(self.fs)}) is available\n")

    def _sync_runs_meta(self, force=False):
        remote_util.remote_activity(f"Refreshing run info for {self.name}")
        self._clear_runs_meta_dir()

        # fs.get fails if a glob doesn't match anything, thus we only add to the list
        # if there is something to get
        meta_globs = []
        if len(self.fs.glob("*/.guild/LOCK")) != 0:
            meta_globs.append("*/.guild/LOCK")
        if len(self.fs.glob("*/.guild/attrs/")) != 0:
            meta_globs.append("*/.guild/attrs/")
        if len(self.fs.glob("*/.guild/opref")) != 0:
            meta_globs.append("*/.guild/opref")

        if len(meta_globs) == 0:
            return
    
        self.fs.get(meta_globs, self._runs_dir, recursive=True, auto_mkdir=True)

    def _clear_runs_meta_dir (self):
        for root, dirs, files in os.walk(self._runs_dir):
            for f in files:
                os.unlink(os.path.join(root, f))
            for d in dirs:
                shutil.rmtree(os.path.join(root, d))
        
    def _purge_runs(self, runs):
        raise NotImplementedError("Pins doesn't support non permanent deletion of runs.")
     
    def _restore_runs(self, runs):
        raise NotImplementedError("Pins doesn't support non permanent deletion of runs.")
    
    def push(self, runs, delete = False):
        remote_util.remote_activity("Pushing runs to pins board...")
        for run in runs:
            self._push_run(run, delete)
        self._sync_runs_meta()

    def _push_run(self, run, delete):
        # is 'put' smart enough to not re-update files that already exist?
        # if not, we should check if the run already exists and skip it
        if self.fs.exists(self._get_run_pin_name(run.id)):
            log.warning("Run %s already exists in pins board. Skipping.", run.id)
            return

        log.info("Pushing run %s", run.id)
        self.fs.put(run.path, self._get_run_pin_name(run.id), recursive=True)
        
    def pull(self, runs, delete=False):
        for run in runs:
            self._pull_run(run, delete)

    def _pull_run(self, run, delete):
        if delete:
            raise ValueError("Unsupported delete op.")
        self.fs.get(self._get_run_pin_name(run.id), os.path.join(var.runs_dir(), run.id), recursive=True, auto_mkdir=True)

    def _delete_runs(self, runs, permanent):
        for run in runs:
            if not permanent:
                log.warning("Deleting pins runs is always permanent. Nothing will be deleted.")
                return
            try:
                self.fs.rm(self._get_run_pin_name(run.id), recursive=True)
            except:
                log.warning("Failed to delete run %s. Unknown error", self._get_run_pin_name(run.id))
    
    def _get_run_pin_name (self, run_id):
        return os.path.join(self.subdir, run_id)
            
def _is_meta_file(name):
    return (
        name.endswith(".guild/opref") or "/.guild/attrs/" in name
        or "/.guild/LOCK" in name
    )
