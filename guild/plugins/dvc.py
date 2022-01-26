# Copyright 2017-2022 TensorHub, Inc.
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

"""TODO:

- Complete correct implementatiom of run support
- Do we care about pipelines vs not?
- Reinstate summaries from metrics
- Flags
  - Import flags from params config
  - Copy + modify config files
- How to handle big files or directories?
- Cleanup / lint (remove unused or commented out code)
- Test concurrency using parallel runs in DvC
- Scenario: run op with specific/multiple data versions (used in pull)

"""

from __future__ import absolute_import
from __future__ import division

import logging
import os
import subprocess

from guild import config
from guild import guildfile
from guild import model as modellib
from guild import plugin as pluginlib
from guild import resolver as resolverlib
from guild import util

from . import dvc_util

log = logging.getLogger("guild")


class _DvcModelProxy(object):

    name = "dvc.yaml"

    def __init__(self, target_stage, project_dir):
        self.modeldef = _init_dvc_modeldef(self.name, target_stage, project_dir)
        self.reference = _init_dvc_model_reference(project_dir)


def _init_dvc_modeldef(model_name, stage_name, project_dir):
    data = [
        {
            "model": model_name,
            "operations": {
                stage_name: _stage_op_data(stage_name, project_dir),
            },
        }
    ]
    gf = guildfile.Guildfile(data, src="<guild.plugins._DvcModelProxy>")
    return gf.models[model_name]


def _stage_op_data(stage_name, project_dir):
    return {
        "main": "guild.plugins.dvc_stage_main --project-dir %s %s"
        % (
            util.shlex_quote(project_dir),
            util.shlex_quote(stage_name),
        ),
        "description": "Stage '%s' imported from dvc.yaml" % stage_name,
        "flags": {
            #     "pipeline": {
            #         "description": (
            #             "Run stage as pipeline. This runs stage dependencies first."
            #         ),
            #         "type": "boolean",
            #         "arg-switch": True,
            #         "default": False,
            #     }
        },
        "sourcecode": {
            "dest": ".",
            "select": [],
        },
    }


def _init_dvc_model_reference(project_dir):
    dvc_yaml_path = os.path.join(project_dir, "dvc.yaml")
    if os.path.isfile(dvc_yaml_path):
        version = modellib.file_hash(dvc_yaml_path)
    else:
        version = "unknown"
    return modellib.ModelRef("import", dvc_yaml_path, version, "dvc.yaml")


class _Stage:
    def __init__(self, name, config, project_dir):
        self.name = name
        self.config = config
        self.project_dir = project_dir


class _DvcFileResolver(resolverlib.Resolver):
    def resolve(self, resolve_context):
        assert self.source.uri.startswith("dvc://"), self.source.uri
        dvc_dep = self.source.uri[6:]
        _pull_dep(dvc_dep, resolve_context.run.dir)


def _pull_dep(dep, cwd):
    cmd = ["dvc", "pull", dep]
    p = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    _out, err = p.communicate()
    if p.returncode != 0:
        raise resolverlib.ResolutionError(
            "error fetching DvC dependency '%s': %s"
            % (dep, err.strip().decode("utf-8", errors="ignore"))
        )


class DvcPlugin(pluginlib.Plugin):
    @staticmethod
    def guildfile_loaded(gf):
        for m in gf.models.values():
            _maybe_apply_dvc_stages(m.extra, m)

    @staticmethod
    def resolve_model_op(opspec):
        if opspec.startswith("dvc.yaml:"):
            target_stage = opspec[9:]
            model = _DvcModelProxy(target_stage, os.path.abspath(config.cwd()))
            return model, target_stage
        return None

    @staticmethod
    def resolver_class_for_url_scheme(scheme):
        if scheme == "dvc":
            return _DvcFileResolver
        return None


def _maybe_apply_dvc_stages(model_config, model):
    for stage in _iter_dvc_stages(model_config, _model_dir(model)):
        _add_or_merge_operation_for_stage(stage, model)


def _model_dir(model):
    return model.guildfile.dir


def _iter_dvc_stages(dvc_config, project_dir):
    stages_import = _coerce_dvc_stages_import(dvc_config.get("dvc-stages-import"))
    if not stages_import:
        return
    dvc_config = dvc_util.load_dvc_config(project_dir)
    for stage_name, stage_config in (dvc_config.get("stages") or {}).items():
        if _filter_dvc_stage(stage_name, stages_import):
            yield _Stage(stage_name, stage_config, project_dir)


def _coerce_dvc_stages_import(val):
    if val is None or isinstance(val, (list, bool)) or val == "all":
        return val
    if isinstance(val, str):
        return [val]
    log.warning(
        "invalid value for 'dvc-stages-import' %r - "
        "expected boolean, 'all', or list of stage names",
        val,
    )
    return None


def _filter_dvc_stage(name, import_spec):
    if import_spec in (True, "all"):
        return True
    if isinstance(import_spec, list) and name in import_spec:
        return True
    return False


def _add_or_merge_operation_for_stage(stage, model):
    opdef = _ensure_stage_opdef(stage, model)
    log.debug("importing DvC stage '%s' as '%s'", stage.name, opdef.fullname)


def _ensure_stage_opdef(stage, model):
    model_opdef = model.get_operation(stage.name)
    stage_opdef = _init_stage_opdef(stage, model)
    if model_opdef:
        _apply_stage_opdef_config(stage_opdef, model_opdef)
        return model_opdef
    else:
        model.operations.append(stage_opdef)
        return stage_opdef


def _init_stage_opdef(stage, model):
    return guildfile.OpDef(
        stage.name,
        _stage_op_data(stage.name, stage.project_dir),
        model,
    )


def _apply_stage_opdef_config(stage_opdef, model_opdef):
    if model_opdef.main:
        log.warning(
            "ignoring operation main attribute %r for DvC stage import",
            model_opdef.main,
        )
    model_opdef.main = stage_opdef.main
    if not model_opdef.description:
        model_opdef.description = stage_opdef.description
    if model_opdef.flags:
        log.warning("ignoring operation flags for DvC stage import")
    model_opdef.flags = list(stage_opdef.flags)
