"""There so many mapping related opts, Putting them all here."""
import logging

from ._shared_options import GLOBAL_TASK_OPTIONS
import pbsmrtpipe.schema_opt_utils as OP

log = logging.getLogger(__name__)


def _to_max_hits():
    return OP.to_option_schema(OP.to_opt_id("max_hits"), "integer", "Max Hits", "Max Hits Description", -1)


def _to_max_error():
    return OP.to_option_schema(OP.to_opt_id("max_error"), "number", "Max Divergence", "Mad Divergence description", -1.0)


def _to_pbalign_opts():
    return OP.to_option_schema(OP.to_opt_id("pbalign_opts"), "string", "PB Align CLI opts", "PBAlign Commandline Options", "")


def _to_pbalign_advanced_opts():
    return OP.to_option_schema(OP.to_opt_id("pbalign_advanced_opts"), "string", "PB Align Advanced CLI opts", "PBAlign Commandline Advanced Options", "")


def _to_min_anchor_size():
    return OP.to_option_schema(OP.to_opt_id("min_anchor_size"), "integer", "Minimum Anchor Size", "Minimum Anchor size Description", -1)


def _to_py_load_pulses():
    return OP.to_option_schema(OP.to_opt_id("py_loadpulses"), "boolean", "Use Python Load Pulses", "Use Python version of Load Pulses", False)


def _to_load_pulses():
    # this should really be an enum
    return OP.to_option_schema(OP.to_opt_id("load_pulses"), "boolean", "Load Pulses", "Load Pulses Description", True)


def _to_load_pulses_opts():
    # this should really be an enum
    return OP.to_option_schema(OP.to_opt_id("load_pulses_opts"), "string", "Load Pulses", "Load Pulses Description", "")


def _to_load_pulses_metric_opts():
    # this should really be an enum
    return OP.to_option_schema(OP.to_opt_id("load_pulses_metrics"), "string", "Load Pulses", "Load Pulses Description", "DeletionQV,IPD,InsertionQV,PulseWidth,QualityValue,MergeQV,SubstitutionQV,DeletionTag")


def _to_noise_opts():
    return OP.to_option_schema(OP.to_opt_id("noise.data"), ("string", "null"), "Load Pulse Options", "Load Pulse Option Descriptions", None)


def _to_divide_by_adapter():
    return OP.to_option_schema(OP.to_opt_id("divide_by_adapter"), "boolean", "Divide By Adapter", "Divide by Adapter Description", False)


def _to_concordant():
    return OP.to_option_schema(OP.to_opt_id("concordant"), "boolean", "Concordant", "Concordant Description", False)


def _to_use_quality():
    return OP.to_option_schema(OP.to_opt_id("use_quality"), "boolean", "Use Quality", "Use Quality Description", False)


def _to_place_repeats_randomly():
    return OP.to_option_schema(OP.to_opt_id("place_repeats_randomly"), "boolean", "Place Repeats Randomly", "Place Repeats Randomly Description", True)


def _to_filter_adapters_only():
    return OP.to_option_schema(OP.to_opt_id("filter_adapters_only"), "boolean", "Filter Adapter Only mode", "Filter Adapter Only mode Description", False)


def _to_sam_read_groups():
    oid = OP.to_opt_id("sam_read_groups")
    return OP.to_option_schema(oid, "string", "SAM Read Groups", "SAM Read Groups Description", "movie")


def _to_raw_blasr_opts():
    oid = OP.to_opt_id("raw_blasr_opts")
    return OP.to_option_schema(oid, ["string", "null"], "Raw BLASR options", "Raw BLASR options string", None)


def _to_d(schema):
    x = schema['properties']
    keys = x.keys()
    oid = keys[0]
    return {oid: schema}


def to_bam_blasr_opts():
    """BAM align makes raw call to blasr"""
    schemas = []
    schemas.append(GLOBAL_TASK_OPTIONS[OP.to_opt_id('use_subreads')])
    opts = [_to_place_repeats_randomly(),
            _to_max_error(),
            _to_min_anchor_size(),
            _to_max_hits(),
            _to_concordant(),
            _to_use_quality(),
            _to_raw_blasr_opts()]

    for opt in opts:
        schemas.append(opt)

    return {s['required'][0]: s for s in schemas}


def to_align_schema_opts():
    schemas = []
    schemas.append(GLOBAL_TASK_OPTIONS[OP.to_opt_id('use_subreads')])

    opts = [_to_pbalign_opts(), _to_min_anchor_size(), _to_max_hits(),
            _to_max_error(), _to_place_repeats_randomly(), _to_concordant(),
            _to_use_quality(), _to_filter_adapters_only(),
            _to_load_pulses(), _to_load_pulses_opts(),
            _to_load_pulses_metric_opts()]

    for opt in opts:
        schemas.append(opt)

    return {s['required'][0]: s for s in schemas}


def _to_blasr_opts(ropts, sa_file=None):
    """
    Keeping some of the old code style here.
    """
    opts = []

    def _add(x):
        opts.append(x)

    f = OP.to_opt_id

    def v(x):
        return ropts[f(x)]

    use_pb_align = True
    header = " -x " if not use_pb_align else " "

    def add_s_option(name, value):
        return '{h}-{n} {v}'.format(h=header, n=name, v=str(value))

    def add_n_option(name, value):
        if value >= 0:
            return add_s_option(name, value)
        else:
            return ""

    _add(add_n_option('minMatch', v('min_anchor_size')))
    _add(add_n_option('bestn', v('max_hits')))

    max_error = v('max_error')
    if max_error >= 0.0:
        _add(add_n_option('minPctIdentity', 100.0 - max_error))

    # FIX THIS MESS
    # if self.reference.info.hasIndexFile('bsdb'):
    #     alignOpts += add_s_option( 'seqdb', self.reference.info.indexFile('bsdb') )

    # FIXME TODO pbalign or blasr should really extract everything from the reference dir
    # # suffix array
    if sa_file is not None:
        _add(add_s_option('sa', sa_file))

    opt_str = " ".join(opts)
    return " --algorithmOptions='{s}'".format(s=opt_str)


def to_align_opts_str(ropts, nproc, tmp_dir, sa_file=None):
    """
    Convert the dulge of arguments to a commandline str

    :param ropts:
    :return:
    """
    # cli string opts
    opts = []

    f = OP.to_opt_id

    def v(x):
        key = f(x)
        if key not in ropts:
            log.error("unable to find '{k}' in resolved opts".format(k=key))
            log.error(ropts)

        return ropts[f(x)]

    def _add(x):
        opts.append(x)

    _add(v('pbalign_opts'))

    blasr_opts = _to_blasr_opts(ropts, sa_file=sa_file)
    # log.debug(blasr_opts)
    _add(blasr_opts)

    # as per mkinsella's suggestion
    _add('--seed=1')

    if not v('use_subreads'):
        _add("--noSplitSubreads")

    if v('place_repeats_randomly'):
        _add("--hitPolicy=randombest")
    else:
        if '--hitPolicy' not in v('pbalign_opts'):
            _add('--hitPolicy=leftmost')

    if v('concordant'):
        _add("--algorithmOptions='-concordant'")

    if v('use_quality'):
        _add("--algorithmOptions='-useQuality'")

    if v('filter_adapters_only'):
        _add("--filterAdapterOnly")

    if tmp_dir is not None:
        _add(" --tmpDir={f}".format(f=tmp_dir))

    _add('-vv')

    if nproc > 0:
        _add('--nproc={s}'.format(s=nproc))

    return " ".join(opts)
