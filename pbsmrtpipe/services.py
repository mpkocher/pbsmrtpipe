"""Utils for Updating state/progress and results to WebServices


"""
import json
import logging
from collections import namedtuple

from pbcore.io import DataSetMetaTypes
import requests

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

# Official updating of job state. The scala jobtype executor will set this
# EventMessage = namedtuple("EventMessage", "")


def log_progress_state(log_service, log_resource, log_message):
    pass


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
    def wrapper(total_url):
        r = rqget(total_url)
        r.raise_for_status()
        j = r.json()
        return func(j)

    return wrapper


def _process_rpost(func):
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
    """Access Layer for interfacing with the Secondary Analysis SMRT Server"""
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

    def log_progress_update(self, job_type_id, job_id, message, level,
                            source_id):
        _d = dict(message=message, level=level, sourceId=source_id)
        return _process_rpost(_null_func)(_to_url(self.uri, "/secondary-analysis/job-manager/jobs/{t}/{i}/log".format(t=job_type_id, i=job_id)), _d)
