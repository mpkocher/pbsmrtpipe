
"""
Tool contract wrappers for miscellaneous quick functions.
"""

import functools
import tempfile
import logging
import shutil
import gzip
import os
import sys

from pbcore.io import SubreadSet, HdfSubreadSet
from pbcommand.engine import run_cmd
from pbcommand.cli import registry_builder, registry_runner
from pbcommand.models import FileTypes

log = logging.getLogger(__name__)

TOOL_NAMESPACE = 'pbsmrtpipe'
DRIVER_BASE = "python -m pbsmrtpipe.pb_tasks.pacbio "

registry = registry_builder(TOOL_NAMESPACE, DRIVER_BASE)


def _run_bax_to_bam(input_file_name, output_file_name):
    base_name = ".".join(output_file_name.split(".")[:-2])
    input_file_name_tmp = input_file_name
    # XXX bax2bam won't write an hdfsubreadset unless the input is XML too
    if input_file_name.endswith(".bax.h5"):
        input_file_name_tmp = tempfile.NamedTemporaryFile(
            suffix=".hdfsubreadset.xml").name
        ds_tmp = HdfSubreadSet(input_file_name)
        ds_tmp.write(input_file_name_tmp)
    args =[
        "bax2bam",
        "--subread",
        "-o", base_name,
        "--output-xml", output_file_name,
        "--xml", input_file_name_tmp
    ]
    logging.info(" ".join(args))
    result = run_cmd(" ".join(args),
                     stdout_fh=sys.stdout,
                     stderr_fh=sys.stderr)
    if result.exit_code != 0:
        return result.exit_code
    tmp = tempfile.NamedTemporaryFile(suffix=".subreadset.xml").name
    shutil.move(output_file_name, tmp)
    # FIXME it would be better to leave this to bax2bam
    with SubreadSet(tmp) as ds:
        if not ds.isIndexed:
            ds.induceIndices()
        ds.write(output_file_name)
    return 0


def run_bax_to_bam(input_file_name, output_file_name):
    with HdfSubreadSet(input_file_name) as ds_in:
        movies = set()
        for rr in ds_in.resourceReaders():
            movies.add(rr.movieName)
        if len(movies) > 1:
            out_dir = os.path.dirname(output_file_name)
            ds_out_files = []
            for bax_file in ds_in.toExternalFiles():
                output_file_name_tmp = os.path.join(out_dir, ".".join(
                    os.path.basename(bax_file).split(".")[:-2]) +
                    ".hdfsubreadset.xml")
                rc = _run_bax_to_bam(bax_file, output_file_name_tmp)
                if rc != 0:
                    logging.error("bax2bam failed")
                    return rc
                ds_out_files.append(output_file_name_tmp)
            ds = SubreadSet(*ds_out_files)
            ds.write(output_file_name)
        else:
            return _run_bax_to_bam(input_file_name, output_file_name)
    return 0


def run_bam_to_fastx(program_name, input_file_name, output_file_name):
    def _splitext(path):
        base, ext = os.path.splitext(path)
        if ext == ".gz":
            base, ext2 = os.path.splitext(base)
            ext = ext2 + ext
        return base, ext
    args = [
        program_name,
        "-o", _splitext(output_file_name)[0],
        input_file_name,
    ]
    logging.info(" ".join(args))
    result = run_cmd(" ".join(args),
                     stdout_fh=sys.stdout,
                     stderr_fh=sys.stderr)
    if result.exit_code != 0:
        return result.exit_code
    else:
        if not output_file_name.endswith(".gz"):
            output_file_name_ = output_file_name + ".gz"
            with gzip.open(output_file_name_) as f_in:
                with open(output_file_name, "w") as f_out:
                    f_out.write(f_in.read())
    return 0

def run_fasta_to_fofn(input_file_name, output_file_name):
    args = ["echo", input_file_name, ">", output_file_name]
    logging.info(" ".join(args))
    result = run_cmd(" ".join(args), stdout_fh = sys.stdout,
                     stderr_fh=sys.stderr)
    return result.exit_code

def run_fasta_to_referenceset(input_file_name, output_file_name):
    # this can be moved out to pbdataset/pbcoretools eventually
    args = ["dataset create", "--type ReferenceSet", "--generateIndices",
            output_file_name, input_file_name]
    logging.info(" ".join(args))
    result = run_cmd(" ".join(args), stdout_fh = sys.stdout,
                     stderr_fh=sys.stderr)
    # the '.py' name difference will be resolved in pbdataset/pbcoretools, but
    # for now, work with either
    if result.exit_code == 127:
        args = ["dataset.py create", "--type ReferenceSet",
                "--generateIndices",
                output_file_name, input_file_name]
        logging.info(" ".join(args))
        result = run_cmd(" ".join(args), stdout_fh = sys.stdout,
                         stderr_fh=sys.stderr)
    return result.exit_code


run_bam_to_fasta = functools.partial(run_bam_to_fastx, "bam2fasta")
run_bam_to_fastq = functools.partial(run_bam_to_fastx, "bam2fastq")


@registry("h5_subreads_to_subread", "0.1.0",
          FileTypes.DS_SUBREADS_H5,
          FileTypes.DS_SUBREADS, is_distributed=True, nproc=1)
def run_bax2bam(rtc):
    return run_bax_to_bam(rtc.task.input_files[0], rtc.task.output_files[0])


@registry("bam2fastq", "0.1.0",
          FileTypes.DS_SUBREADS,
          FileTypes.FASTQ, is_distributed=True, nproc=1)
def run_bam2fastq(rtc):
    return run_bam_to_fastq(rtc.task.input_files[0], rtc.task.output_files[0])


@registry("bam2fasta", "0.1.0",
          FileTypes.DS_SUBREADS,
          FileTypes.FASTA, is_distributed=True, nproc=1)
def run_bam2fasta(rtc):
    return run_bam_to_fasta(rtc.task.input_files[0], rtc.task.output_files[0])

@registry("fasta2fofn", "0.1.0",
          FileTypes.FASTA,
          FileTypes.FOFN, is_distributed=False, nproc=1)
def run_fasta2fofn(rtc):
    return run_fasta_to_fofn(rtc.task.input_files[0], rtc.task.output_files[0])

@registry("fasta2referenceset", "0.1.0",
          FileTypes.FASTA,
          FileTypes.DS_REF, is_distributed=True, nproc=1)
def run_fasta2referenceset(rtc):
    return run_fasta_to_referenceset(rtc.task.input_files[0],
                                     rtc.task.output_files[0])

@registry("bam2fastq_ccs", "0.1.0",
          FileTypes.DS_CCS,
          FileTypes.FASTQ, is_distributed=True, nproc=1)
def run_bam2fastq_ccs(rtc):
    """
    Duplicate of run_bam2fastq, but with ConsensusReadSet as input.
    """
    return run_bam_to_fastq(rtc.task.input_files[0], rtc.task.output_files[0])


@registry("bam2fasta_ccs", "0.1.0",
          FileTypes.DS_CCS,
          FileTypes.FASTA, is_distributed=True, nproc=1)
def run_bam2fasta_ccs(rtc):
    """
    Duplicate of run_bam2fasta, but with ConsensusReadSet as input.
    """
    return run_bam_to_fasta(rtc.task.input_files[0], rtc.task.output_files[0])


if __name__ == '__main__':
    sys.exit(registry_runner(registry, sys.argv[1:]))
