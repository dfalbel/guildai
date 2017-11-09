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

import os
import time
import uuid

import yaml

import guild.util

class Run(object):

    __properties__ = [
        "id",
        "path",
        "short_id",
        "pid",
        "status"
    ]

    def __init__(self, id, path):
        self.id = id
        self.path = path
        self._guild_dir = os.path.join(self.path, ".guild")

    @property
    def short_id(self):
        return self.id[:8]

    @property
    def pid(self):
        lockfile = self.guild_path("LOCK")
        try:
            raw = open(lockfile, "r").read()
        except (IOError, ValueError):
            return None
        else:
            return int(raw)

    @property
    def status(self):
        remote_lock = self.guild_path("LOCK.remote")
        if os.path.exists(remote_lock):
            return "running"
        pid = self.pid
        if pid is None:
            if self.get("exit_status") == 0:
                return "completed"
            else:
                return "error"
        elif guild.util.pid_exists(pid):
            return "running"
        else:
            return "terminated"

    def get(self, name, default=None):
        try:
            return self[name]
        except KeyError:
            return default

    def iter_attrs(self):
        for name in sorted(os.listdir(self._attrs_dir())):
            try:
                yield name, self[name]
            except KeyError:
                pass

    def __getitem__(self, name):
        try:
            f = open(self._attr_path(name), "r")
        except IOError:
            raise KeyError(name)
        else:
            return yaml.safe_load(f)

    def _attr_path(self, name):
        return os.path.join(self._attrs_dir(), name)

    def _attrs_dir(self):
        return os.path.join(self._guild_dir, "attrs")

    def __repr__(self):
        return "<guild.run.Run '%s'>" % self.id

    def init_skel(self):
        guild.util.ensure_dir(self.guild_path("attrs"))
        guild.util.ensure_dir(self.guild_path("logs"))

    def guild_path(self, subpath):
        return os.path.join(self._guild_dir, subpath)

    def write_attr(self, name, val):
        encoded = yaml.safe_dump(
            val,
            default_flow_style=False,
            indent=2).strip()
        if encoded.endswith("\n..."):
            encoded = encoded[:-4]
        with open(self._attr_path(name), "w") as f:
            f.write(encoded)
            f.close()

    def iter_files(self, all_files=False, include_dirs=False):
        for root, dirs, files in os.walk(self.path, followlinks=True):
            if not all_files and root == self.path:
                try:
                    dirs.remove(".guild")
                except ValueError:
                    pass
            if include_dirs:
                for name in dirs:
                    yield os.path.join(root, name)
            for name in files:
                yield os.path.join(root, name)

def timestamp():
    """Returns an integer use for run timestamps."""
    return int(time.time() * 1000000)

def timestamp_seconds(ts):
    """Returns seconds float from value generated by `timestamp`."""
    return float(ts / 1000000)

def mkid():
    return uuid.uuid1().hex
