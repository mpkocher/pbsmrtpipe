#!/usr/bin/env python
"""
# This is for pbsmrtpipe (and subdependency, pbcommand) ONLY. DO NOT put random
# SA# tools in here. They should NOT be assumed to be installed.
"""
import os
import shlex
import sys
import subprocess
import itertools
import multiprocessing
from pbcommand.engine import run_cmd

_EMIT = '--emit-tool-contract'
TC_DIR = 'pbsmrtpipe/register_tool_contracts'
SA3_TC_DIR = 'pbsmrtpipe/pb_static_tasks'
TASKS_ROOT = 'pbsmrtpipe/tools_dev'

PBCOMMAND_TCS = ('dev_txt_app',
                 'dev_app',
                 'dev_gather_fasta_app',
                 'dev_scatter_fasta_app')


def _to_tc_cmd(input_module, output_file):
    return "python -m {i} {e} > {o}".format(i=input_module, e=_EMIT, o=output_file)


def _to_registry_cmd(input_file):
    return "python -m {i} emit-tool-contracts -o {o}".format(i=input_file, o=TC_DIR)


def create_all_pbsmrtpipe_tcs(root_dir, output_dir):
    cmds = []
    for file_name in os.listdir(root_dir):
        if not file_name.startswith('__'):
            name = os.path.splitext(file_name)[0]
            file_name = 'pbsmrtpipe.tools_dev.' + name + "_tool_contract.json"
            output_file = os.path.join(output_dir, file_name)
            module_name = 'pbsmrtpipe.tools_dev.' + name
            cmd = _to_tc_cmd(module_name, output_file)
            cmds.append(cmd)

    return cmds


def quick_registry(output_dir):
    cmds = []
    x = cmds.append
    x('python -m pbsmrtpipe.pb_tasks.dev emit-tool-contracts -o {o}'.format(o=output_dir))
    x('python -m pbcommand.cli.examples.dev_quick_hello_world emit-tool-contracts -o {o}'.format(o=output_dir))
    return cmds


def create_pbcommand_tcs(output_dir):
    cmds = []
    for i in PBCOMMAND_TCS:
        module_name = 'pbcommand.cli.examples.' + i
        output_name = 'dev_pbcommand_{i}_tool_contract.json'.format(i=i)
        cmd = _to_tc_cmd(module_name, os.path.join(output_dir, output_name))
        cmds.append(cmd)
    return cmds


def sa3_tool(output_dir):
    cmds = []
    x = cmds.append
    x('python -m pbsmrtpipe.pb_tasks.pacbio emit-tool-contracts -o {o}'.format(o=output_dir))
    return cmds


def _run_cmd(cmd):
    print cmd
    result = run_cmd(cmd, sys.stdout, sys.stderr)
    if result.exit_code != 0:
        print result
        raise ValueError("Failed to generate TC from {c}".format(c=cmd))
    return result


def create_all():
    to_cmds = [create_all_pbsmrtpipe_tcs(TASKS_ROOT, TC_DIR),
               create_pbcommand_tcs(TC_DIR),
               quick_registry(TC_DIR),
               sa3_tool(SA3_TC_DIR)]

    cmds = itertools.chain(*to_cmds)
    p = multiprocessing.Pool(min(multiprocessing.cpu_count(), 4))
    results = p.map(_run_cmd, cmds)
    return 0


if __name__ == '__main__':
    sys.exit(create_all())
