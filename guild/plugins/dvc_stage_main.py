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

from __future__ import absolute_import
from __future__ import division

import argparse

##import json
import logging
import os
import re
import subprocess

import yaml

##from guild import config
from guild import op_util

##from guild import steps_util
from guild import run as runlib

##from guild import summary
from guild import util
from guild import vcs_util

##from guild.commands import run_impl

from . import dvc_util

log = None

PIPELINE_STAGE_PS = [
    re.compile(r"Stage '(.*?)' didn't change, skipping"),
    re.compile(r"Running stage '(.*?)':"),
]

DVC_META_NAMES = ["dvc.yaml", "dvc.lock"]


class _Pipeline:
    def __init__(self, target_stage, project_dir):
        _assert_dvc_yaml(project_dir)
        self.target_stage = target_stage
        self.project_dir = project_dir
        self.run_dir = _required_run_dir()
        self.run = runlib.for_dir(self.run_dir)
        self.ran_stages = []
        self.dvc_yaml = _load_dvc_yaml(project_dir)
        self.parent_run = op_util.current_run()


def _assert_dvc_yaml(project_dir):
    dvc_yaml = os.path.join(project_dir, "dvc.yaml")
    if not os.path.exists(dvc_yaml):
        _missing_dvc_yaml_error(project_dir)


def _required_run_dir():
    run_dir = os.getenv("RUN_DIR")
    if not run_dir:
        raise SystemExit("missing required environment RUN_DIR for operation")
    return run_dir


def _load_dvc_yaml(dir):
    with open(os.path.join(dir, "dvc.yaml")) as f:
        return yaml.safe_load(f)


def main():
    op_util.init_logging()
    globals()["log"] = logging.getLogger("guild")
    args = _init_args()
    pipeline = _Pipeline(args.stage, args.project_dir)
    if args.pipeline:
        assert False, "TODO: What to do here? Do we need this?"
        ##_handle_pipeline(pipeline)
    else:
        _handle_stage(pipeline)


def _init_args():
    p = argparse.ArgumentParser()
    p.add_argument("--pipeline", action="store_true")
    p.add_argument("--project-dir")
    p.add_argument("stage")
    args = p.parse_args()
    _ensure_project_dir_arg(args)
    return args


def _ensure_project_dir_arg(args):
    if not args.project_dir:
        project_dir_env = os.getenv("PROJECT_DIR")
        if not project_dir_env:
            raise SystemExit(
                "unspecified project dir - specify either --project-dir "
                "or set the PROJECT_DIR environment variable"
            )
        args.project_dir = project_dir_env


def _missing_dvc_yaml_error(project_dir):
    raise SystemExit(
        "invalid DvC config directory '%s' - missing dvc.yaml" % project_dir
    )


# def _handle_pipeline(pipeline):
#     for stage in _iter_pipeline_stages(pipeline):
#         _assert_not_ran_pipeline_stage(stage, pipeline)
#         _run_pipeline_stage(stage, pipeline)
#         pipeline.ran_stages.append(stage)
#     _assert_target_stage_ran(pipeline)


# def _iter_pipeline_stages(pipeline):
#     out = _repro_dry_run_for_pipeline(pipeline)
#     for line in out.split("\n"):
#         stage = _pipeline_stage_for_line(line)
#         if stage:
#             yield stage


# def _pipeline_stage_for_line(line):
#     for p in PIPELINE_STAGE_PS:
#         m = p.match(line)
#         if m:
#             return m.group(1)
#     return None


# def _repro_dry_run_for_pipeline(pipeline):
#     cmd = [
#         "dvc",
#         "repro",
#         "--dry",
#         pipeline.target_stage,
#     ]
#     log.debug("dvc repro dry run cmd: %s", cmd)
#     p = subprocess.Popen(
#         cmd,
#         cwd=pipeline.project_dir,
#         stdout=subprocess.PIPE,
#         stderr=subprocess.PIPE,
#     )
#     out, err = p.communicate()
#     log.debug(
#         "dvc repro dry run result:\n%s---\n%s---\n%i",
#         util.lazy_str(lambda: _decode(out)),
#         util.lazy_str(lambda: _decode(err)),
#         p.returncode,
#     )
#     if p.returncode != 0:
#         _dvc_pipeline_status_error(err, p.returncode)
#     assert not err, (err, out)
#     return _decode(out)


# def _decode(s):
#     return s.decode("utf-8", errors="ignore") if s is not None else None


# def _dvc_pipeline_status_error(err_raw, returncode):
#     assert err_raw is not None
#     err = _decode(err_raw).strip()
#     raise SystemExit("error getting DvC pipeline status: %s (%i)" % (err, returncode))


# def _assert_not_ran_pipeline_stage(stage, pipeline):
#     if stage in pipeline.ran_stages:
#         ran_desc = ", ".join(pipeline.ran_stages)
#         raise SystemExit(
#             "cycle detected in pipeline stages: trying to run "
#             "'%s' but already ran %s" % (stage, ran_desc)
#         )


# def _run_pipeline_stage(stage, pipeline):
#     stage_run = _init_stage_run(stage, pipeline)
#     env = _stage_env()
#     flags = ("copy-deps=%s" % ("yes" if pipeline.copy_deps else "no"),)
#     with config.SetCwd(pipeline.project_dir):
#         with util.Env(env, replace=True):
#             run_impl.run(
#                 opspec="dvc.yaml:%s" % stage,
#                 flags=flags,
#                 run_dir=stage_run.dir,
#             )


# def _init_stage_run(stage, pipeline):
#     stage_run = steps_util.init_step_run(pipeline.parent_run.dir)
#     steps_util.link_to_step_run(stage, stage_run.dir, pipeline.parent_run.dir)
#     return stage_run


# def _stage_env():
#     env = dict(os.environ)
#     env["NO_WARN_RUNDIR"] = "1"
#     return env


# def _assert_target_stage_ran(pipeline):
#     if pipeline.target_stage not in pipeline.ran_stages:
#         ran_desc = ", ".join(pipeline.ran_stages)
#         raise SystemError(
#             "failed to run target stage '%s' in pipeline %s"
#             % (pipeline.target_stage, ran_desc)
#         )


def _handle_stage(pipeline):
    log.info("Running DvC stage %s", pipeline.target_stage)
    _init_run_dir(pipeline)
    _repro_run(pipeline)
    _cleanup_run_dir(pipeline)
    # _copy_dvc_meta_to_run_dir(pipeline)
    # _copy_deps_to_run_dir(pipeline)
    # _copy_outputs_to_run_dir(pipeline)
    # _copy_metrics_to_run_dir(pipeline)


def _init_run_dir(pipeline):
    log.info("Initializing run")
    _write_run_attrs(pipeline)
    _init_vcs_repo(pipeline)
    _init_dvc_repo(pipeline)
    _xxx_pull_special(pipeline)
    copied = _copy_project_source(pipeline)
    _link_to_dvc_cache(pipeline)
    # _resolve_deps(pipeline, copied)
    _copy_params_with_flags(pipeline)


def _xxx_pull_special(pipeline):
    for name in os.listdir(pipeline.run_dir):
        if not name.endswith(".dvc"):
            continue
        dep = name[:-4]
        if not dep:
            continue
        project_path = os.path.join(pipeline.project_dir, dep)
        if os.path.exists(project_path):
            _copy_or_link_project_file(dep, pipeline)
        else:
            log.info("Fetching %s", dep)
            subprocess.check_call(["dvc", "pull", dep], cwd=pipeline.run_dir)


def _write_run_attrs(pipeline):
    pipeline.run.write_attr("dvc-stage", pipeline.target_stage)


def _init_vcs_repo(pipeline):
    _assert_is_git_based(pipeline.project_dir)
    _ = subprocess.check_output(
        ["git", "init"],
        stderr=subprocess.STDOUT,
        cwd=pipeline.run_dir,
    )


def _assert_is_git_based(project_dir):
    git_marker = os.path.join(project_dir, ".dvc", ".gitignore")
    if not os.path.exists(git_marker):
        raise SystemExit("Cannot run DvC stages in %s - requires Git repository")


def _init_dvc_repo(pipeline):
    _ = subprocess.check_output(
        ["dvc", "init"],
        stderr=subprocess.STDOUT,
        cwd=pipeline.run_dir,
    )
    util.copyfile(
        os.path.join(pipeline.project_dir, ".dvc", "config"),
        os.path.join(pipeline.run_dir, ".dvc", "config"),
    )


def _copy_project_source(pipeline):
    copied = set()
    for path in vcs_util.iter_source_files(pipeline.project_dir):
        src = os.path.join(pipeline.project_dir, path)
        if not os.path.exists(src):
            continue
        dest = os.path.join(pipeline.run_dir, path)
        if os.path.exists(dest):
            continue
        log.debug("copying %s to %s", src, dest)
        util.ensure_dir(os.path.dirname(dest))
        util.copyfile(src, dest)
        copied.add(path)
    return copied


def _link_to_dvc_cache(pipeline):
    dvc_src = os.path.join(pipeline.project_dir, ".dvc")
    dvc_dest = os.path.join(pipeline.run_dir, ".dvc")
    for name in ("tmp", "cache"):
        src = os.path.join(dvc_src, name)
        if not os.path.exists(src):
            continue
        util.ensure_dir(dvc_dest)
        rel_target = os.path.relpath(src, dvc_dest)
        link = os.path.join(dvc_dest, name)
        if os.path.exists(link):
            util.safe_rmtree(link)
        util.symlink(rel_target, link)


def _resolve_deps(pipeline, copied):
    for dep_stage, deps in _target_stage_deps(pipeline):
        deps = _filter_not_in(deps, copied)
        if dep_stage:
            _resolve_stage_deps(dep_stage, deps, pipeline)
        else:
            _resolve_project_deps(deps, pipeline)


def _target_stage_deps(pipeline):
    deps = {}
    for dep, dep_stage in dvc_util.iter_stage_deps(
        pipeline.target_stage, pipeline.dvc_yaml
    ):
        deps.setdefault(dep_stage, []).append(dep)
    return sorted(deps.items(), key=_str_or_none_key)


def _str_or_none_key(x):
    if x[0] is None:
        return '', x[1]
    return x


def _filter_not_in(xs, excluded):
    return [x for x in xs if not x in excluded]


def _resolve_stage_deps(stage, deps, pipeline):
    stage_run = dvc_util.marked_or_latest_run_for_stage(pipeline.project_dir, stage)
    if not stage_run:
        _no_suitable_run_for_stage_error(stage, deps)
    log.info("Using %s for '%s' DvC stage dependency", stage_run.id, stage)
    _link_op_deps(stage_run, deps, pipeline)


def _no_suitable_run_for_stage_error(stage, deps):
    deps_desc = ", ".join(sorted(deps))
    raise SystemExit(
        "no suitable run for stage '%s' (needed for %s)" % (stage, deps_desc)
    )


def _link_op_deps(run, deps, pipeline):
    for dep in deps:
        target = os.path.join(run.dir, dep)
        rel_target = os.path.relpath(target, pipeline.run_dir)
        link = os.path.join(pipeline.run_dir, dep)
        log.info("Linking %s", dep)
        util.ensure_dir(os.path.dirname(link))
        util.symlink(rel_target, link)


def _resolve_project_deps(deps, pipeline):
    for dep in deps:
        if _is_project_file(dep, pipeline):
            _copy_or_link_project_file(dep, pipeline)
        else:
            _pull_dep(dep, pipeline)


def _is_project_file(dep, pipeline):
    path = os.path.join(pipeline.project_dir, dep)
    return os.path.exists(path)


def _copy_or_link_project_file(dep, pipeline):
    dep_path = os.path.join(pipeline.project_dir, dep)
    if _can_copy_dep(dep_path):
        _copy_project_file(dep_path, dep, pipeline)
    else:
        _link_project_file(dep_path, dep, pipeline)


def _can_copy_dep(dep_path):
    return os.path.isfile(dep_path)


def _copy_project_file(src, dep, pipeline):
    dest = os.path.join(pipeline.run_dir, dep)
    log.info("Copying %s", dep)
    util.copyfile(src, dest)


def _link_project_file(src, dep, pipeline):
    link = os.path.join(pipeline.run_dir, dep)
    rel_src = os.path.relpath(src, os.path.dirname(link))
    log.info("Linking to %s", dep)
    util.symlink(rel_src, link)


def _pull_dep(dep, pipeline):
    cmd = ["dvc", "pull", dep]
    log.info("Fetching %s", dep)
    p = subprocess.Popen(cmd, cwd=pipeline.run_dir)
    returncode = p.wait()
    if returncode != 0:
        raise SystemExit(
            "'dvc pull' failed (exit code %i) - see above for details" % returncode
        )


def _copy_params_with_flags(pipeline):
    for name in _iter_stage_param_files(pipeline):
        log.info("Copying %s", name)
        src = os.path.join(pipeline.project_dir, name)
        dest = os.path.join(pipeline.run_dir, name)
        util.copyfile(src, dest)


def _iter_stage_param_files(pipeline):
    seen = set()
    for _param, filename in dvc_util.iter_stage_params(
        pipeline.target_stage, pipeline.dvc_yaml
    ):
        if not filename in seen:
            yield filename
            seen.add(filename)


def _repro_run(pipeline):
    cmd = ["dvc", "repro", "--single-item", pipeline.target_stage]
    p = subprocess.Popen(cmd, cwd=pipeline.run_dir)
    returncode = p.wait()
    if returncode != 0:
        raise SystemExit(
            "'dvc repro %s' failed (exit code %i) - see above for details"
            % (util.shlex_quote(pipeline.target_stage), returncode)
        )


def _cleanup_run_dir(pipeline):
    _rm_vcs_repo(pipeline.run_dir)


def _rm_vcs_repo(dir):
    vcs_repo = os.path.join(dir, ".git")
    log.debug("deleting VCS repo %s", vcs_repo)
    util.safe_rmtree(vcs_repo)


# def _copy_dvc_meta_to_run_dir(pipeline):
#     source_dir = pipeline.project_dir
#     run_dir = _required_run_dir()
#     for name in DVC_META_NAMES:
#         src = os.path.join(source_dir, name)
#         if not os.path.exists(src):
#             log.warning(
#                 "DvC meta file %s does not exist in %s - "
#                 "cannot copy to Guild run directory",
#                 name,
#                 source_dir,
#             )
#             continue
#         log.debug("copying stage output '%s' to run directory '%s'", src, run_dir)
#         log.info("Copying %s", name)
#         util.copyutil(src, run_dir)


# def _copy_deps_to_run_dir(pipeline):
#     source_dir = pipeline.project_dir
#     deps = _target_stage_deps(pipeline)
#     run_dir = _required_run_dir()
#     for name in deps:
#         if pipeline.copy_deps:
#             _copy_stage_file(name, source_dir, run_dir, "dep")
#         _copy_stage_file("%s.dvc", source_dir, run_dir, "dvc file", if_exists=True)


# def _copy_stage_file(name, source_dir, dest_dir, desc, if_exists=False):
#     src = os.path.join(source_dir, name)
#     if not os.path.exists(src):
#         if not if_exists:
#             log.warning(
#                 "Stage %s '%s' does not exist - cannot copy to Guild run directory",
#                 desc,
#                 name,
#             )
#         return
#     log.debug("copying stage %s '%s' to run directory '%s'", desc, src, dest_dir)
#     log.info("Copying %s", name)
#     util.copyfile(src, dest_dir)


# def _copy_outputs_to_run_dir(pipeline):
#     source_dir = pipeline.project_dir
#     outputs = _target_stage_outputs(pipeline)
#     run_dir = _required_run_dir()
#     for name in outputs:
#         _copy_stage_file(name, source_dir, run_dir, "output")


# def _target_stage_outputs(pipeline):
#     stage_data = pipeline.dvc_yaml.get("stages", {}).get(pipeline.target_stage, {})
#     return stage_data.get("outs", [])


# def _copy_metrics_to_run_dir(pipeline):
#     source_dir = pipeline.project_dir
#     metrics = _target_stage_metrics(pipeline)
#     run_dir = _required_run_dir()
#     with summary.SummaryWriter(run_dir) as events:
#         for name in metrics:
#             _copy_stage_file(name, source_dir, run_dir, "metrics")
#             _write_metrics_as_scalars(name, run_dir, events)


# def _target_stage_metrics(pipeline):
#     stage_data = pipeline.dvc_yaml.get("stages", {}).get(pipeline.target_stage, {})
#     for x in stage_data.get("metrics", []):
#         if isinstance(x, str):
#             yield x
#         elif isinstance(x, dict):
#             for name in x:
#                 yield name
#         else:
#             log.warning(
#                 "unexpected metrics value %r for stage '%s', skipping",
#                 x,
#                 pipeline.target_stage,
#             )


# def _write_metrics_as_scalars(metrics_name, run_dir, events):
#     try:
#         data = _load_metrics(os.path.join(run_dir, metrics_name))
#     except Exception as e:
#         log.warning("error reading metrics from %s: %s", metrics_name, e)
#     else:
#         for tag, val in _iter_metrics_scalars(data):
#             events.add_scalar(tag, val)


# def _load_metrics(path):
#     ext = os.path.splitext(path)[1]
#     if ext in (".yml", "yaml"):
#         return _load_yaml(path)
#     elif ext in (".json",):
#         return _load_json(path)
#     else:
#         raise ValueError("unsupported metrics type in %s" % os.path.basename(path))


# def _load_yaml(path):
#     return yaml.safe_load(open(path))


# def _load_json(path):
#     return json.load(open(path))


# def _iter_metrics_scalars(data):
#     if not isinstance(data, dict):
#         return
#     flattened_data = util.encode_nested_config(data)
#     for name, val in flattened_data.items():
#         if isinstance(val, (int, float)):
#             yield name, val


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        op_util.handle_system_exit(e)
