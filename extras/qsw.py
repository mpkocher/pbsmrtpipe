#!/usr/bin/env python

""" Simple wrapper for qsub which provides the functionality of the -sync
option"""

import sys
import os
import re
import subprocess
import time
import logging
import tempfile

__version__ = 1.0

# CONFIGURATION

# max number of times QSTAT will be polled
MAX_QSTAT_FAILURES = 3
# time between calls to qstat (in sec)
POLLING_INTERVAL = 5
# Max polling time to for wait for exitcode file (sec) default is 5 minutes
MAX_POLLING_TIME = 60 * 1.5 

# regex to match output from scheduler
PBS_JOB_ID_DECODER = "(\d+)\..+"

PBS_NO_JOB_FOUND_CODE = 153
PBS_SUCCESS_CODE = 0

# Unique str (built from a palindrome) to extract exitcode from wrapper script
EXIT_CODE_ENCODER = "PB_EXIT_CODE_${?}_EDOC_TIXE_BP"
EXIT_CODE_DECODER = "PB_EXIT_CODE_(\d+)_EDOC_TIXE_BP"

# internal debugging flag for testing your configuration.
DEBUG = True
#DEBUG = False

log = logging.getLogger(__name__)


def _setupLog(file_name=None):
    if file_name is None:
        handler = logging.StreamHandler(sys.stdout)
    else:
        handler = logging.FileHandler(file_name)

    str_formatter = '[%(levelname)s] %(asctime)-15s [%(name)s %(funcName)s %(lineno)d] %(message)s'
    formatter = logging.Formatter(str_formatter)
    handler.setFormatter(formatter)
    log.addHandler(handler)
    log.setLevel(logging.DEBUG)


class QSubError(Exception):
    pass


class QSubWrapper(object):

    def __init__(self):
        self.scriptToRun = None
        self.jobIdDecoder = None
        self.noJobFoundCode = None
        self.qstatCmd = None
        self.successCode = None
        self.tmpScriptPath = None
        self.qsubArgs = None
        self.debug = False

        # This will properly initialize the instance vars
        self.__parseArgs()

    def __parseArgs(self):
        """Handle command line argument parsing"""
        args = sys.argv[1:]

        log.debug("Parsing args {a}".format(a=args))

        if len(args) < 2:
            sys.stderr.write("qsw.py script.sh [qsub options]\n")
            sys.exit(-1)

        self.scriptToRun = args[0]

        if not os.path.exists(self.scriptToRun):
            sys.stderr.write("Unable to find {f}\n".format(f=self.scriptToRun))
            # Standard FileNotFound return code
            sys.exit(256)

        self.tmpScriptPath = os.path.splitext(self.scriptToRun)[0] + ".wrap.sh"

        # We need this option in order to function correctly.
        if '-S' not in args:
            args.append('-S')
            args.append('/bin/bash')

        # Check if we are wrapping PBS or SGE (SGE support is removed)
        if '-PBS' in args:
            args.remove('-PBS')
        elif '-SGE' in args:
            sys.stderr.write("SGE support is disabled\n")
            sys.exit(1)
        self.jobIdDecoder = PBS_JOB_ID_DECODER
        self.noJobFoundCode = PBS_NO_JOB_FOUND_CODE
        self.successCode = PBS_SUCCESS_CODE
        self.qstatCmd = "qstat"

        self.qsubArgs = " ".join(args[1:])

    def _writeWrapperScript(self, childScript, parentScript):
        """
        Generates a wrapper(parent) script which calls the child script and
        pipes its exit code to a file.
        Returns the path to this file.
        """

        base, _ = os.path.splitext(childScript)

        exitCodePath = base + '.exit'
        # this will be the actual stdout and stderr of the task
        # this will write the task log directory
        log_base = base.replace('/workflow/', '/log/')
        std_out = log_base + '.stdout'
        std_err = log_base + '.stderr'

        with open(parentScript, 'w') as out:
            out.write("# Wrapper script for %s\n" % childScript)
            out.write("/bin/bash {c} > {o} 2> {e}\n".format(c=childScript, o=std_out, e=std_err))
            # MK: This might be a better/simpler solution to directly output the returncode?
            #outFile.write("echo $? > exitCodePath")
            out.write("echo \"%s\" > %s;\n" % (EXIT_CODE_ENCODER, exitCodePath))

        log.debug("Completed writing wrapper script to {x}".format(x=exitCodePath))
        return exitCodePath

    def _waitForJobTermination(self, jobId):
        """
        Loop until we no longer see the job in qstat or we hit a bunch of
        qstat failures.
        """
        log.info("waiting for jobId {i} to complete".format(i=jobId))

        consecutiveFailures = 0

        while True:
            # Push this error up to smrtpipe level and useful for
            # debugging/testing
            e_out = tempfile.NamedTemporaryFile(suffix='.err')
            o_out = tempfile.NamedTemporaryFile(suffix='.out')

            # Check job status via qstat/qstat -j 1234
            cmd = "%s %s" % (self.qstatCmd, jobId)

            log.debug("calling cmd {c}".format(c=cmd))
            retCode = subprocess.call(cmd, shell=True,
                                      stderr=e_out,
                                      stdout=o_out)

            e_out.seek(0)
            error_str = e_out.read()
            if error_str:
                # this should just be qstat related errors
                # (e.g., unable to get server, timeout)
                # Most of the time this will catch Errors like:
                # PBS: 'qstat: Unknown Job Id 38976.localhost.localdomain'
                log.warn(error_str)
                sys.stderr.write(error_str + "\n")

            if DEBUG:
                o_out.seek(0)
                out_str = o_out.read()
                log.debug(out_str)

            failed = retCode not in [self.noJobFoundCode, self.successCode]
            consecutiveFailures = consecutiveFailures + 1 if failed else 0

            if consecutiveFailures >= MAX_QSTAT_FAILURES:
                msg = "Unable to query qstat for job status (job %s). Failed %d times." % (jobId, consecutiveFailures)
                log.error(msg)
                raise QSubError(msg)

            if retCode == self.noJobFoundCode:
                # Job is not indentified by qstat (qstat returns non-zero exit
                # code). Hence the job is done.
                log.info("Breaking. Found returncode {r}".format(r=retCode))
                e_out.close()
                o_out.close()
                break

            time.sleep(POLLING_INTERVAL)
        log.info("Completed waiting for termination of jobId {i}".format(i=jobId))

    def _extractExitCode(self, fileName):
        """
        Extracts the exit code written to the specified path of the
        wrapper script.
        """
        # fileName might still not show up immediately, so wait for the file to
        # appear.

        log.debug("extracting exit code from {f}".format(f=fileName))
        nAttempts = 0
        startedAt = time.time()
        runTime = time.time() - startedAt

        while runTime < MAX_POLLING_TIME:
            # need to careful when this is computed due to try/catch
            runTime = time.time() - startedAt
            nAttempts += 1

            # Attempt to force an NFS Refresh
            os.listdir(os.path.dirname(fileName))
            try:
                with open(fileName, 'r') as inFile:
                    match = re.search(EXIT_CODE_DECODER, inFile.read())

                if match:
                    exitCode = int(match.group(1))
                else:
                    msg = "Unable to extract exit code from file %s" % fileName
                    raise QSubError(msg)
                return exitCode

            except Exception as e:
                print "Attempt {n} still waiting for {f}. Got error {e}. runtime {r}".format(n=nAttempts, f=fileName, e=str(e), r=runTime)

            time.sleep(POLLING_INTERVAL)

        # if we haven't been able to see the file by now, raise an Exception
        raise QSubError("Unable to extract exit code from file {f}".format(f=fileName))

    def run(self):
        """
        Submits the command using qsub. Monitors progress using qstat.
        Returns with the exit code of the job

        :return: (int) Returncode
        """

        exitCodePath = self._writeWrapperScript(self.scriptToRun, self.tmpScriptPath)

        try:
            cmd = "qsub %s %s" % (self.qsubArgs, self.tmpScriptPath)
            log.info("calling cmd : {c}".format(c=cmd))
            output = subprocess.check_output(cmd, shell=True)

            match = re.search(self.jobIdDecoder, output)

            if match:
                jobId = int(match.group(1))
            else:
                msg = "Unable to derive jobId from qsub output ({s}) using pattern ({x})".format(s=output, x=self.jobIdDecoder)
                raise QSubError(msg)

            # This will block
            self._waitForJobTermination(jobId)

            exitCode = self._extractExitCode(exitCodePath)
            # propogate the exitCode to sys.exit()
            log.info("exiting {f} with returncode {r}".format(f=__file__,
                                                              r=exitCode))
            return exitCode

        except KeyboardInterrupt:
            # FIXME: jobId might not be defined yet
            cmd = "qdel %s" % jobId
            retCode = subprocess.call(cmd, shell=True)

            if retCode != self.successCode:
                msg = "Unable to qdel running job %s. You may have to kill it manually" % jobId
                raise QSubError(msg)


if __name__ == "__main__":
    if DEBUG:
        file_name = os.path.join(os.getcwd(), 'wrapper.log')
        _setupLog()
        log.info("Running {f} version {v}".format(f=__file__, v=__version__))
    app = QSubWrapper()
    sys.exit(app.run())
