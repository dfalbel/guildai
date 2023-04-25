import logging
import shutil
import tempfile
import os
import zipfile
import sys
import pins
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
        board_config = config['config']
        if board_config['board'] == "temp":
            self.board = pins.board_temp()
        elif board_config['board'] == 'folder':
            self.board = pins.board_folder(board_config['path'])
        else:
            raise RuntimeError(f"Unsupported board configuration {board_config['path']}.")

        self.local_env = remote_util.init_env(config.get("local-env"))
        self.local_sync_dir = meta_sync.local_meta_dir(name, str(board_config))
        runs_dir = os.path.join(self.local_sync_dir, *RUNS_PATH)
        deleted_runs_dir = os.path.join(self.local_sync_dir, *DELETED_RUNS_PATH)
        super().__init__(runs_dir, deleted_runs_dir)

    def status(self, verbose=False):
        sys.stdout.write(f"{self.name} (Pins board {str(self.board)}) is available\n")

    def _sync_runs_meta(self, force=False):
        remote_util.remote_activity(f"Refreshing run info for {self.name}")
        runs = self.board.pin_search(as_df=True)
        self._clear_runs_meta_dir()
        for _, run in runs.iterrows():
            meta_string = run.meta.user["guild_meta"]
            with tempfile.NamedTemporaryFile(mode= "wb") as temp:
                _ = temp.write(bytes(meta_string))
                temp.flush()
                with zipfile.ZipFile(temp.name, mode="r") as zip_ref:
                    path = os.path.join(self._runs_dir, run["name"], "")
                    zip_ref.extractall(path)

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
        with tempfile.TemporaryDirectory() as outdir:
            archive_path = self._archive_run_dir(run.path, outdir)
            metadata = self._archive_run_meta(run.path)
            # TODO Currently pins doesn't support uploading files, so we read the archive into a binary
            # string and use pin_write to upload it.
            with open(archive_path, mode="rb") as f:
                data = list(f.read())
            self.board.pin_write(data, type="json", name=run.id, versioned=True, metadata={"guild_meta": metadata})

    def _archive_run_dir(self, dir, outfile):
       return shutil.make_archive(outfile, 'zip', dir)
    
    def _archive_run_meta(self, run_path):
        with tempfile.NamedTemporaryFile() as tmp:
            with zipfile.ZipFile(tmp.name, "w") as zf:
                for root, _, files in os.walk(run_path):
                    for file in files:
                        fname = os.path.join(root, file)
                        relpath = os.path.relpath(fname, run_path)
                        if _is_meta_file(fname):    
                            zf.write(fname, relpath)
            # now read the zip file into a bytestring
            with open(tmp.name, "rb") as f:
                data = f.read()
            return data
        
    def pull(self, runs, delete=False):
        for run in runs:
            self._pull_run(run, delete)

    def _pull_run(self, run, delete):
        if delete:
            raise ValueError("Unsupported delete op.")
        archive = self.board.pin_read(run.id)
        with tempfile.NamedTemporaryFile(mode= "wb") as temp:
            _ = temp.write(bytes(archive))
            with zipfile.ZipFile(temp.name, mode="r") as zip_ref:
                    zip_ref.extractall(os.path.join(var.runs_dir(), run.id, ""))

    def _delete_runs(self, runs, permanent):
        for run in runs:
            if not permanent:
                log.warning("Deleting pins runs is always permanent. Nothing will be deleted.")
                return
            try:
                self.board.pin_delete(run.id)
            except pins.errors.PinsError as e:
                log.warning("Failed to delete run %s: %s", run.id, e)
            except:
                log.warning("Failed to delete run %s. Unknown error", run.id)
            
def _is_meta_file(name):
    return (
        name.endswith(".guild/opref") or "/.guild/attrs/" in name
        or "/.guild/LOCK" in name
    )
