# Copyright 2017-2022 RStudio, PBC
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

import click

from guild import click_util

from . import api_support


@click.command("ops")
@api_support.output_options
@click_util.use_args
@click_util.render_doc
def main(args):
    """Show operations as JSON."""

    api_support.out(_ops_data(), args)


def _ops_data():
    from guild import cmd_impl_support
    from . import operations_impl

    ops_args = click_util.Args(installed=False, all=False, filters=[])
    cmd_impl_support.init_model_path()
    return [
        _strip_private(op_data) for op_data in operations_impl.filtered_ops(ops_args)
    ]


def _strip_private(data):
    if not isinstance(data, dict):
        return data
    return {key: _strip_private(val) for key, val in data.items() if key[:1] != "_"}
