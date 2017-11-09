# Copyright 2017 TensorHub, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import
from __future__ import division

import argparse
import json
import os
import re
import shlex
import subprocess
import sys

from guild import cli
from guild import plugin
from guild import plugin_util
from guild import util

BACKGROUND_SYNC_INTERVAL = 60
BACKGROUND_SYNC_STOP_TIMEOUT = 10

class Train(object):

    def __init__(self, args, log):
        job_args, flag_args = self._parse_args(args)
        self.run = plugin_util.current_run()
        self.job_name = job_args.job_name or self._job_name()
        self.job_dir = "gs://%s/%s" % (job_args.bucket_name, self.job_name)
        self.runtime_version = job_args.runtime_version
        self.module_name = job_args.module_name
        self.package_path = job_args.package_path
        self.region = job_args.region
        self.flag_args = flag_args
        self.package_name = self._package_name()
        self.package_version = self._package_version()
        self.log = log

    @staticmethod
    def _parse_args(args):
        p = argparse.ArgumentParser()
        p.add_argument("--region", required=True)
        p.add_argument("--bucket-name", required=True)
        p.add_argument("--job-name")
        p.add_argument("--runtime-version")
        p.add_argument("--module-name", required=True)
        p.add_argument("--package-path")
        p.add_argument("--data-dir")
        return p.parse_known_args(args)

    def _job_name(self):
        return "guild_run_%s" % self.run.id

    def _package_name(self):
        from guild.opref import OpRef
        opref = OpRef.from_run(self.run)
        return opref.model_name

    def _package_version(self):
        return "0.0.0+%s" % self.run.short_id

    def __call__(self):
        self._write_run_attrs()
        self._upload_files()
        self._init_package()
        self._submit_job()
        self._write_lock()
        self._watch_logs()
        self._sync()

    def _write_run_attrs(self):
        self.run.write_attr("cloudml-job-name", self.job_name)
        self.run.write_attr("cloudml-job-dir", self.job_dir)

    def _upload_files(self):
        for name in os.listdir(self.run.path):
            if name == ".guild":
                continue
            src = os.path.join(self.run.path, name)
            dest = self.job_dir + "/" + name
            self._recursive_copy_files(src, dest)

    @staticmethod
    def _recursive_copy_files(src, dest):
        subprocess.check_call(
            ["/usr/bin/gsutil", "-m", "cp", "-r", src, dest])

    def _init_package(self):
        env = {
            "PYTHONPATH": os.path.pathsep.join(sys.path),
            "PACKAGE_NAME": self.package_name,
            "PACKAGE_VERSION": self.package_version,
        }
        # Use an external process as setuptools assumes it's a command
        # line app
        subprocess.check_call(
            [sys.executable, "-um", "guild.plugins.training_pkg_main"],
            env=env,
            cwd=self.run.path)

    def _submit_job(self):
        args = [
            "/usr/bin/gcloud", "ml-engine", "jobs",
            "submit", "training", self.job_name,
            "--job-dir", self.job_dir,
            "--packages", self._find_package_name(),
            "--module-name", self.module_name,
            "--region", self.region,
        ]
        if self.runtime_version:
            args.extend(["--runtime-version", self.runtime_version])
        if self.package_path:
            args.extend(["--package-path", self.package_path])
        if self.flag_args:
            args.append("--")
            args.extend(self._resolved_flag_args())
        self.log.info("Starting job %s in %s", self.job_name, self.job_dir)
        self.log.debug("gutil cmd: %r", args)
        try:
            subprocess.check_call(args)
        except subprocess.CalledProcessError as e:
            sys.exit(e.returncode)

    def _resolved_flag_args(self):
        subs = [
            ("${job-dir}", self.job_dir),
            ("${job-name}", self.job_name),
        ]
        def resolve(val):
            for pattern, sub in subs:
                val = val.replace(pattern, sub)
            return val
        return [resolve(arg) for arg in self.flag_args]

    def _find_package_name(self):
        package_name = re.sub(r"[^0-9a-zA-Z]+", "_", self.package_name)
        path = "%s-%s-py2.py3-none-any.whl" % (package_name, self.package_version)
        assert os.path.exists(path), path
        return path

    def _write_lock(self):
        with open(self.run.guild_path("LOCK.remote"), "w") as f:
            f.write("cloudml")

    def _watch_logs(self):
        args = [
            "/usr/bin/gcloud", "ml-engine", "jobs",
            "stream-logs", "--polling-interval", "10",
            self.job_name
        ]
        background_sync = BackgroundSync(self.run, self.log)
        background_sync.start()
        try:
            subprocess.check_call(args)
        except KeyboardInterrupt:
            cli.out("Stopping job monitor")
        finally:
            background_sync.stop()
            sys.stdout.write("\n")
            sys.stdout.flush()

    def _sync(self):
        sync = Sync(self.run, self.log)
        sync()

class BackgroundSync(util.LoopingThread):

    def __init__(self, run, log):
        sync = Sync(run, log)
        super(BackgroundSync, self).__init__(
            cb=sync.__call__,
            interval=BACKGROUND_SYNC_INTERVAL,
            stop_timeout=BACKGROUND_SYNC_STOP_TIMEOUT)

class Sync(object):

    def __init__(self, run, log):
        self.run = run
        self.log = log

    def __call__(self):
        self._sync_files()
        self._sync_status()

    def _sync_files(self):
        job_dir = self.run.get("cloudml-job-dir")
        if not job_dir:
            self.log.error(
                "cloudml-job-dir not defined for run %s, cannot sync files",
                self.run.id)
            return
        cli.out("Synchronizing job output for run %s" % self.run.id)
        self._rsync_files(job_dir, self.run.path)

    def _rsync_files(self, src, dest):
        try:
            subprocess.check_call(
                ["/usr/bin/gsutil", "-m", "rsync", "-r", src, dest])
        except subprocess.CalledProcessError:
            self.log.error(
                "error syncing run %s files from %s (see above for details)",
                self.run.id, src)

    def _sync_status(self):
        job_name = self.run.get("cloudml-job-name")
        if not job_name:
            self.log.error(
                "cloudml-job-name not defined for run %s, cannot sync status",
                self.run.id)
            return
        cli.out("Synchronizing job status for run %s" % self.run.id)
        info = self._job_info(job_name)
        state = info.get("state")
        cli.out("Run %s is %s" % (self.run.id, state))
        self.run.write_attr("cloudml-job-state", state)
        if state not in ["RUNNING", "PREPARING"]:
            self._finalize_run(state)

    @staticmethod
    def _job_info(job_name):
        out = subprocess.check_output(
            ["/usr/bin/gcloud", "--format", "json", "ml-engine", "jobs",
             "describe", job_name])
        return json.loads(out)

    def _finalize_run(self, state):
        cli.out("Finalizing run %s" % self.run.id)
        exit_status = self._exit_status_for_job_state(state)
        if exit_status is not None:
            self.run.write_attr("exit_status", exit_status)
            self._delete(self.run.guild_path("LOCK"))
            self._delete(self.run.guild_path("LOCK.remote"))

    def _exit_status_for_job_state(self, state):
        if state == "SUCCEEDED":
            return 0
        elif state == "FAILED":
            return 1
        else:
            self.log.warning(
                "got unexpected job state '%s' for run %s",
                state, self.run.id)
            return None

    def _delete(self, filename):
        try:
            os.remove(filename)
        except OSError as e:
            if os.path.exists(filename):
                self.log.error(
                    "could not delete '%s' from run %s (%s)",
                    filename, self.run.id, e)

class CloudMLPlugin(plugin.Plugin):

    def enabled_for_op(self, op):
        parts = shlex.split(op.cmd)
        if parts[0] != "@cloudml:train":
            return False, "operation not supported by plugin"
        return True, ""

    def run_op(self, op_spec, args):
        if op_spec == "train":
            self._train(args)
        else:
            raise plugin.NotSupported(op_spec)

    def _train(self, args):
        train = Train(args, self.log)
        train()

    def sync_run(self, run, _lock_config):
        sync = Sync(run, self.log)
        sync()
