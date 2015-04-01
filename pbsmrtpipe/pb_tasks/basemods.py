import logging
import os

from pbsmrtpipe.core import register_task
from pbsmrtpipe.models import TaskTypes, FileTypes, ResourceTypes, SymbolTypes
import pbsmrtpipe.schema_opt_utils as OP


log = logging.getLogger(__name__)


def _to_opt(id_, types_, name_, desc_, default_):
    return OP.to_option_schema(OP.to_opt_id(id_), types_, name_, desc_, default_)


def _to_max_length():
    return OP.to_option_schema(OP.to_opt_id("basemods.max_length"), ("integer", "null"), "Max Length", "Max Length Description", None)


def _to_identify():
    return OP.to_option_schema(OP.to_opt_id("basemods.identify_modifications"), "boolean", "Identify Modifications", "Identify Modifications Description", False)


def _to_map_qv_threshold():
    return OP.to_option_schema(OP.to_opt_id("basemods.enable_map_qv_threshold"), ("string", "null"), "Map QV Threshold", "Base Modification Detection Map QV Threshold", None)


def _to_compute_methyl_fraction():
    return OP.to_option_schema(OP.to_opt_id("basemods.compute_methyl_fraction"), "boolean", "Compute Methyl Fraction", "Base Modification Compute Methyl Fraction", True)


def _to_tet_treaded():
    return OP.to_option_schema(OP.to_opt_id('basemods.tet_treaded'), "boolean", "Tetra", "Tetra", False)


def _get_compute_modification_opts():
    _opts = [_to_max_length(), _to_identify(), _to_map_qv_threshold(),
             _to_compute_methyl_fraction(), _to_tet_treaded()]
    return {opt['required'][0]: opt for opt in _opts}


@register_task("pbsmrtpipe.tasks.compute_modifications", TaskTypes.DISTRIBUTED,
               (FileTypes.FASTA, FileTypes.REPORT, FileTypes.ALIGNMENT_CMP_H5, FileTypes.FOFN),
               (FileTypes.GFF, FileTypes.CSV, FileTypes.ALIGNMENT_CMP_H5),
               _get_compute_modification_opts(),
               SymbolTypes.MAX_NPROC, (),
               output_file_names=(("modifications", "gff"),
                                  ("modifications", "csv"),
                                  ("alignment_with_kinetics", "h5")))
def to_cmd(input_files, output_files, ropts, nproc, resources):
    # this should also call copy ipdSummary?

    _max_length = ropts[OP.to_opt_id('basemods.max_length')]
    max_length = ''
    if _max_length is not None:
        # this can only be > 0
        max_length = "--maxLength x".format(x=_max_length)

    methyl_fraction = ''
    if ropts[OP.to_opt_id('basemods.compute_methyl_fraction')]:
        methyl_fraction = ' --methylFraction '

    map_qv = ''
    _mqv = ropts[OP.to_opt_id('basemods.enable_map_qv_threshold')]
    if _mqv is not None:
        map_qv = " --mapQvThreshold {x} ".format(x=_mqv)

    identify = ''
    if ropts[OP.to_opt_id('basemods.identify_modifications')] is True:
        tet_treaded = ropts[OP.to_opt_id("basemods.tet_treaded")]
        if tet_treaded:
            identify = " --identify m6A,m4C "
        else:
            identify = " --identify m6A,m4C,m5C_TET "

    _d = dict(w=input_files[3], m=methyl_fraction, i=identify, q=map_qv, n=nproc,
              g=output_files[0], c=output_files[1], h=output_files[2],
              r=input_files[0], x=max_length, a=input_files[2])
    cmd = "ipdSummary.py -v -W {w} {m} {i} {q} {x} --numWorkers {n} --summary_h5 {h} --gff {g} --csv {c} --reference {r} {a}"

    return cmd.format(**_d)


@register_task("pbsmrtpipe.tasks.modification_report", TaskTypes.DISTRIBUTED,
               FileTypes.CSV, FileTypes.REPORT,
               {}, 1, (),
               output_file_names=(("modifications_report", "json"),))
def to_cmd(input_files, output_files, ropts, nproc, resources):
    # this should also call copy ipdSummary?
    cmds = []

    # FIXME. The python layer should support a gzip'ed or non-gzip'ed CSV
    input_gz = os.path.join(os.path.dirname(output_files[0]), os.path.basename(input_files[0]) + ".gz")
    cmds.append("gzip -c {i} > {f}".format(f=input_gz, i=input_files[0]))

    exe = 'pbreport.py modifications --debug'
    o = os.path.dirname(output_files[0])
    r = os.path.basename(output_files[0])
    cmd = "{e} {o} {r} {c}".format(e=exe, o=o, r=r, c=input_files[0])
    cmds.append(cmd)
    return cmds


@register_task("pbsmrtpipe.tasks.add_modification_to_summary", TaskTypes.DISTRIBUTED,
               (FileTypes.GFF, FileTypes.GFF),
               FileTypes.GFF,
                {}, 1, (),
               mutable_files=(("$inputs.1", "$outputs.0"),))
def to_cmd(input_files, output_files, ropts, nproc, resources):
    exe = "summarizeModifications.py"
    cmd_str = "{e} --modifications {i} --alignmentSummary {s} --outfile %s"
    _d = dict(e=exe, i=input_files[1], s=input_files[0], o=output_files[0])
    return cmd_str.format(**_d)


@register_task('pbsmrtpipe.tasks.find_motifs', TaskTypes.DISTRIBUTED,
               (FileTypes.GFF, FileTypes.FASTA),
               (FileTypes.CSV, FileTypes.XML),
               {}, SymbolTypes.MAX_NPROC, (),
               output_file_names=(('motif_summary', 'csv'), ('motif_summary_report', '.xml')))
def to_cmd(input_files, output_files, ropts, nproc, resources):

    _d = dict(g=input_files[0], r=input_files[1], c=output_files[0], x=output_files[1])
    cmd = "motifMaker.sh find --gff {g} --fasta {r} --output {c} --xml {x}"

    return cmd.format(**_d)


@register_task('pbsmrtpipe.tasks.make_motif_gff', TaskTypes.DISTRIBUTED,
               (FileTypes.GFF, FileTypes.CSV, FileTypes.CSV, FileTypes.FASTA),
               FileTypes.GFF, {}, 1,
               (ResourceTypes.TMP_FILE,),
               output_file_names=(('motifs', 'gff.gz'),))
def to_cmd(input_files, output_files, ropts, nproc, resources):
    """
    Inputs types: Modification GFF, Modification CSV, Motif CSV, Reference Fasta
    """

    cmds = []
    _d = dict(c=input_files[0], g=input_files[1], f=input_files[3],
              m=input_files[2], t=resources[0])
    cmd = "motifMaker.sh reprocess -m {c} --gff {g} --fasta {f} --csv {c}  --output {t}"
    cmds.append(cmd.format(**_d))
    cmds.append("gzip --no-name -c {t} > {o}".format(t=resources[0], o=output_files[0]))
    return cmds


@register_task('pbsmrtpipe.tasks.motif_report', TaskTypes.DISTRIBUTED,
               (FileTypes.GFF, FileTypes.CSV), FileTypes.REPORT,
               {}, 1, (),
               output_file_names=(('motif_report', 'json'),))
def to_cmd(input_files, output_files, ropts, nproc, resources):
    """
    Modification GFF, Modification CSVMotif CSV, Mod

    """

    report_base_name = os.path.basename(output_files[0])
    report_path = os.path.dirname(output_files[0])

    exe = 'motifs_report.py'
    _d = dict(e=exe,
              g=input_files[0],
              c=input_files[1],
              r=report_base_name,
              o=report_path)
    cmd = "{e} {g} {c} --output={o} --report_json={r}".format(**_d)
    return cmd
