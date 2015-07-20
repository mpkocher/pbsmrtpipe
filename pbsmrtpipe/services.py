"""Utils for Updating state/progress and results to WebServices


"""
import json
import logging
from collections import namedtuple

from pbcore.io import DataSetMetaTypes
import requests
import time

log = logging.getLogger(__name__)


# This are mirrored from the BaseSMRTServer
class LogLevels(object):
    TRACE = "TRACE"
    DEBUG = "DEBUG"
    INFO = "INFO"
    NOTICE = "NOTICE"
    WARN = "WARN"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"
    FATAL = "FATAL"

    ALL = (TRACE, DEBUG, INFO, NOTICE, WARN, ERROR, CRITICAL, FATAL)

    @classmethod
    def is_valid(cls, level):
        return level in cls.ALL


HEADERS = {'Content-type': 'application/json'}
SERVICE_LOGGER_RESOURCE_ID = "pbsmrtpipe"

LogResource = namedtuple("LogResource", "id name description")
LogMessage = namedtuple("LogMessage", "sourceId level message")

PbsmrtpipeLogResource = LogResource(SERVICE_LOGGER_RESOURCE_ID, "Pbsmrtpipe",
                                    "Secondary Analysis Pbsmrtpipe Job logger")


class JobExeError(ValueError):
    """Service Job Failure"""
    pass

# "Job" is the raw output from the jobs/1234
JobResult = namedtuple("JobResult", "job run_time errors")


class ServiceEntryPoint(object):
    """Entry Points to initialize Pipelines"""
    def __init__(self, entry_id, dataset_type, path_or_uri):
        self.entry_id = entry_id
        self.dataset_type = dataset_type
        # int (only supported), UUID or path to XML dataset will be added
        self._resource = path_or_uri

    @property
    def resource(self):
        return self._resource

    def __repr__(self):
        return "<{k} {e} {d} {r} >".format(k=self.__class__.__name__, e=self.entry_id, r=self._resource, d=self.dataset_type)


class JobStates(object):
    RUNNING = "RUNNING"
    CREATED = "CREATED"
    FAILED = "FAILED"
    SUCCESSFUL = "SUCCESSFUL"

    ALL = (RUNNING, CREATED, FAILED)

    # End points
    ALL_COMPLETED = (FAILED, SUCCESSFUL)


class JobTypes(object):
    IMPORT_DS = "import-dataset"
    IMPORT_DSTORE = "import-datastore"
    PB_PIPE = "pbsmrtpipe"
    MOCK_PB_PIPE = "mock-pbsmrtpipe"


class ServiceResourceTypes(object):
    REPORTS = "reports"
    DATASTORE = "datastore"


def _post_requests(headers):
    def wrapper(url, d_):
        data = json.dumps(d_)
        return requests.post(url, data=data, headers=headers)

    return wrapper


def _get_requests(headers):
    def wrapper(url):
        return requests.get(url, headers=headers)

    return wrapper


rqpost = _post_requests(HEADERS)
rqget = _get_requests(HEADERS)


def _process_rget(func):
    # apply the tranform func to the output of GET request if it was successful
    def wrapper(total_url):
        r = rqget(total_url)
        r.raise_for_status()
        j = r.json()
        return func(j)

    return wrapper


def _process_rpost(func):
    # apply the transform func to the output of POST request if it was successful
    def wrapper(total_url, payload_d):
        r = rqpost(total_url, payload_d)
        r.raise_for_status()
        j = r.json()
        return func(j)

    return wrapper


def _to_url(base, ext):
    return "".join([base, ext])


def _null_func(x):
    # Pass thorough func
    return x


def _import_dataset_by_type(dataset_type_id):
    def wrapper(total_url, path):
        _d = dict(datasetType=dataset_type_id, path=path)
        f = _process_rpost(_null_func)
        return f(total_url, _d)

    return wrapper


class ServiceAccessLayer(object):
    """General Access Layer for interfacing with the job types on Secondary SMRT Server"""
    def __init__(self, base_url, port):
        self.base_url = base_url
        self.port = port

    @property
    def uri(self):
        return "{b}:{u}".format(b=self.base_url, u=self.port)

    def _to_url(self, rest):
        return _to_url(self.uri, rest)

    def __repr__(self):
        return "<{k} {u} >".format(k=self.__class__.__name__, u=self.uri)

    def get_status(self):
        """Get status of the server"""
        return _process_rget(_null_func)(_to_url(self.uri, "/status"))

    def get_job_by_id(self, job_id):
        """Get a Job by int id"""
        return _process_rget(_null_func)(_to_url(self.uri, "/secondary-analysis/job-manager/jobs/{i}".format(i=job_id)))

    def get_job_by_type_and_id(self, job_type, job_id):
        return _process_rget(_null_func)(_to_url(self.uri, "/secondary-analysis/job-manager/jobs/{t}/{i}".format(i=job_id, t=job_type)))

    def _get_job_resource_type(self, job_type, job_id, resource_type_id):
        # grab the datastore or the reports
        _d = dict(t=job_type, i=job_id,r=resource_type_id)
        return _process_rget(_null_func)(_to_url(self.uri,"/secondary-analysis/job-manager/jobs/{t}/{i}/{r}".format(**_d)))

    def _import_dataset(self, dataset_type, path):
        # This returns a job resource
        return _import_dataset_by_type(dataset_type)("/secondary-analysis/job-manager/jobs/import-dataset", path)

    def import_dataset_subread(self, path):
        return self._import_dataset(DataSetMetaTypes.SUBREAD, path)

    def import_dataset_hdfsubread(self, path):
        return self._import_dataset(DataSetMetaTypes.HDF_SUBREAD, path)

    def import_dataset_reference(self, path):
        return self._import_dataset(DataSetMetaTypes.REFERENCE, path)

    def create_logger_resource(self, idx, name, description):
        _d = dict(id=idx, name=name, description=description)
        return _process_rpost(_null_func)(_to_url(self.uri, "/loggers"), _d)

    def log_progress_update(self, job_type_id, job_id, message, level, source_id):
        """This is the generic job logging mechanism"""
        _d = dict(message=message, level=level, sourceId=source_id)
        return _process_rpost(_null_func)(_to_url(self.uri, "/secondary-analysis/job-manager/jobs/{t}/{i}/log".format(t=job_type_id, i=job_id)), _d)

    def get_pipeline_template_by_id(self, pipeline_template_id):
        return _process_rget(_null_func)(_to_url(self.uri, "/secondary-analysis/pipeline-templates/{i}".format(i=pipeline_template_id)))

    def create_by_pipeline_template_id(self, name, pipeline_template_id, epoints):
        """Runs a pbsmrtpipe pipeline by pipeline template id"""
        # sanity checking to see if pipeline is valid
        _ = self.get_pipeline_template_by_id(pipeline_template_id)

        seps = [dict(entryId=e.entry_id, fileTypeId=e.dataset_type, datasetId=e.resource) for e in epoints]

        def _to_o(opt_id, opt_value):
            return dict(optionId=opt_id, value=opt_value)
        task_options = [_to_o("option_01", "value_01")]
        workflow_options = [_to_o("woption_01", "value_01")]
        d = dict(name=name, pipelineId=pipeline_template_id, entryPoints=seps, taskOptions=task_options, workflowOptions=workflow_options)
        return _process_rpost(_null_func)(_to_url(self.uri, "/secondary-analysis/job-manager/jobs/{p}".format(p=JobTypes.PB_PIPE)), d)

    def run_by_pipeline_template_id(self, name, pipeline_template_id, epoints, time_out=600):
        """Blocks and runs a job with a timeout"""

        job = self.create_by_pipeline_template_id(name, pipeline_template_id, epoints)
        job_id = job['id']

        def _to_ascii(v):
            return v.encode('ascii', 'ignore')

        state = _to_ascii(job['state'])

        job_result = JobResult(job, 0, "")
        started_at = time.time()
        # in seconds
        sleep_time = 2
        while True:
            run_time = time.time() - started_at
            if state in JobStates.ALL_COMPLETED:
                break
            log.debug("Running pipeline {n} state: {s} runtime:{r:.2f} sec".format(n=name, s=state, r=run_time))

            time.sleep(sleep_time)
            job = self.get_job_by_id(job_id)
            state = _to_ascii(job['state'])
            job_result = JobResult(job, run_time, "")
            if run_time > time_out:
                raise JobExeError("Exceeded runtime {r} of {t}".format(r=run_time, t=time_out))

        return job_result


def log_pbsmrtpipe_progress(total_url, message, level, source_id, ignore_errors=True):
    """Log the status of a pbsmrtpipe to SMRT Server"""

    # Need to clarify the model here. Trying to pass the most minimal
    # data necessary to pbsmrtpipe.
    _d = dict(message=message, level=level, sourceId=source_id)
    func = _process_rpost(_null_func)
    if ignore_errors:
        try:
            return func(total_url, _d)
        except Exception as e:
            log.warn("Failed Request to {u} data: {d}. {e}".format(u=total_url, d=_d, e=e))
    else:
        return func(total_url, _d)


def add_datastore_file(total_url, datastore_file, ignore_errors=True):
    """Add datastore to SMRT Server

    :type datastore_file: DataStoreFile
    """
    _d = datastore_file.to_dict()
    func = _process_rpost(_null_func)
    if ignore_errors:
        try:
            return func(total_url, _d)
        except Exception as e:
            log.warn("Failed Request to {u} data: {d}. {e}".format(u=total_url, d=_d, e=e))
    else:
        return func(total_url, _d)

