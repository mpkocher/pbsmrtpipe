import os
import logging

from pbsmrtpipe.core import register_task, MetaTaskBase
from pbsmrtpipe.models import TaskTypes, FileTypes
import pbsmrtpipe.schema_opt_utils as OP


log = logging.getLogger(__name__)


@register_task('pbsmrtpipe.tasks.input_xml_to_fofn',
               TaskTypes.LOCAL,
               FileTypes.INPUT_XML,
               (FileTypes.MOVIE_FOFN, FileTypes.REPORT),
               {}, 1, (),
               output_file_names=(('input', 'fofn'), ('movie_report', 'json')))
def to_cmd(input_files, output_files, ropts, nproc, resources):
    """Maybe this new report just just be the overview report?"""
    _d = dict(i=input_files[0], o=output_files[0], r=output_files[1])
    return "pbtools-converter input-xml-to-fofn {i} {o} --report {r}".format(**_d)


@register_task('pbsmrtpipe.tasks.movie_overview_report',
               TaskTypes.DISTRIBUTED,
               FileTypes.MOVIE_FOFN,
               FileTypes.REPORT,
               {}, 1, (),
               output_file_names=(('overview_report', 'json'), ))
def to_cmd(input_files, output_files, ropts, nproc, resources):
    _d = dict(i=input_files[0], o=output_files[0])
    return "overview_report.py --debug {i} {o}".format(**_d)


@register_task('pbsmrtpipe.tasks.adapter_report',
               TaskTypes.DISTRIBUTED,
               FileTypes.MOVIE_FOFN,
               FileTypes.REPORT,
               {}, 1, ('$outputdir', ),
               output_file_names=(('adapter_report', 'json'), ))
def to_cmd(input_files, output_files, ropts, nproc, resources):
    exe = "pbreport.py adapter"
    # the report must be give as a file name
    # Leaving this for now. But all os.* should NOT be allowed.
    json_file = os.path.basename(output_files[0])
    report_dir = os.path.dirname(output_files[0])
    d = dict(e=exe, o=report_dir, r=json_file, f=input_files[0])
    cmd = "{e} {o} {r} {f}".format(**d)
    return cmd


def _to_reference_converter_opts():
    opts = []

    def _add(opt):
        opts.append(opt)

    _add(OP.to_option_schema(OP.to_opt_id('reference_processor.sawriter'), 'string', "SA Writer", 'Reference Processor Suffix Array writer', "sawriter -blt 8 -welter"))
    _add(OP.to_option_schema(OP.to_opt_id('reference_processor.sam_idx'), ('null', 'string'), 'SAM index', 'Reference Processor SAM Index', "samtools faidx"))

    _add(OP.to_option_schema(OP.to_opt_id('reference_processor.ref_name'), 'string', 'Reference Name', 'Reference Name', "my_ref"))

    _add(OP.to_option_schema(OP.to_opt_id('reference_processor.organism_name'), ('null', 'string'), 'Organism Name', 'Reference Organism Name', None))
    _add(OP.to_option_schema(OP.to_opt_id('reference_processor.ploidy'), ('null', 'string'), 'Organism ploidy', 'Reference Organism ploidy', None))

    _add(OP.to_option_schema(OP.to_opt_id('reference_processor.gmapdb'), ('null', 'string'), 'Reference GMAPdb', 'Reference GMAP db', None))

    return {opt['required'][0]: opt for opt in opts}


class FastaReferenceInfoConverter(MetaTaskBase):
    """Convert a FASTA file to a Pacbio reference. Using the reference.info.xml"""
    TASK_ID = 'pbsmrtpipe.tasks.reference_converter'
    NAME = "Reference Converter"
    VERSION = '0.2.0'

    TASK_TYPE = TaskTypes.DISTRIBUTED
    INPUT_TYPES = [(FileTypes.FASTA, 'fasta', 'Raw Fasta File')]
    OUTPUT_TYPES = [(FileTypes.REF_ENTRY_XML, 'reference_info', "Pacbio Reference Info XML"),
                    (FileTypes.REPORT, 'rpt', 'Pacbio Reference metadata Report')]
    OUTPUT_FILE_NAMES = [('reference_info', 'xml'), ('reference_report', 'json')]

    SCHEMA_OPTIONS = _to_reference_converter_opts()
    NPROC = 1
    RESOURCE_TYPES = None

    @staticmethod
    def to_cmd(input_files, output_files, ropts, nproc, resources):
        """
        This has a slightly odd interface because it's writing to a directory
        that is not determined by the workflow.

        The approach.

        If 'reference_processor.reference_dir is not None, the reference is
        written there and a symlink is made to make sure the output of the task is satisfied.

        Otherwise, the reference is written to place decided by the workflow.

        reference_uploader
        """
        # the java code might do some name mangling?
        name = ropts[OP.to_opt_id('reference_processor.ref_name')]

        # output_dir_value = ropts[OP.to_opt_id('reference_processor.smrtanalysis_reference_dir')]
        output_dir = os.path.join(os.path.dirname(output_files[0]), 'reference_dir')
        reference_dir = os.path.join(output_dir, name)
        ref_info_xml = os.path.join(reference_dir, 'reference.info.xml')

        def __get_none_opt(ropts, ropt_id, opt_str):
            value = ropts[OP.to_opt_id(ropt_id)]
            render_str_opt = ''
            if value is not None:
                render_str_opt = "".join(['', opt_str, '"' + str(value) + '"', ""])

            return render_str_opt

        def _get_opt(ropt_id_, str_opt_):
            return __get_none_opt(ropts, ropt_id_, str_opt_)

        sawriter = _get_opt('reference_processor.sawriter', '--saw=')
        user = ''
        jobId = ''
        organism = _get_opt('reference_processor.organism_name', '--organism=')
        ploidy = _get_opt('reference_processor.ploidy', '--ploidy=')
        gmapdb = _get_opt('reference_processor.gmapdb', '--gmapdb=')
        custom_opts = ''
        sam_idx = _get_opt('reference_processor.sam_idx', '--samIdx=')

        writeIndex = True
        indexOpt = " " if writeIndex else " --skipIndexUpdate "

        # this is to be able to write to the directory becasue of perforce nonsense
        cmds = ['mkdir -p {f} && chmod u+rwx -R {f}'.format(f=output_dir)]

        exe = 'referenceUploader'
        cmd = '{e} {i} -c --name="{n}" --fastaFile="{f}" --refRepos="{x}" {c} {o} {w} {s} {u} {j} {p} {g} {r} --verbose'
        _d = dict(e=exe, i=indexOpt, n=name, c=custom_opts, o=organism, w=sawriter,
                  s=sam_idx, u=user, j=jobId, p=ploidy, g=gmapdb, r=output_dir, f=input_files[0], x=output_dir)

        cmds.append(cmd.format(**_d))

        cmds.append("pbtools-converter ref-to-report --debug {i} {o}".format(i=reference_dir, o=output_files[1]))

        if output_dir is not None:
            cmds.append("ln -s {o} {d}".format(o=ref_info_xml, d=output_files[0]))

        return cmds
