# coding:utf-8
import os
import logging
import subprocess
import datetime
import json

from tabulate import tabulate

from captain_comeback.activity.messages import (NewCgroupMessage,
                                                StaleCgroupMessage,
                                                RestartCgroupMessage,
                                                RestartTimeoutMessage,
                                                ExitMessage)
from captain_comeback.activity.status import PROC_STATUSES_RAW

KB = 1024

logger = logging.getLogger()


# http://stackoverflow.com/questions/19654578/
class Utc(datetime.tzinfo):
    def tzname(self):
        return "UTC"

    def utcoffset(self, _dt):
        return datetime.timedelta(0)


class ActivityEngine(object):
    def __init__(self, activity_dir, activity_queue):
        self.activity_dir = activity_dir
        self.activity_queue = activity_queue

    def run(self):
        logger.info("ready to process activity")
        while True:
            msg = self.activity_queue.get()

            if isinstance(msg, NewCgroupMessage):
                self._log_activity(msg.cg.name(), "container has started")
            elif isinstance(msg, StaleCgroupMessage):
                self._log_activity(msg.cg.name(), "container has exited")
            elif isinstance(msg, RestartCgroupMessage):
                table_data = [
                    [
                        pinfo["pid"],
                        pinfo["ppid"], # many of troublesome processes are celery workers, which process trees up to 4 levels deep
                        pinfo["memory_info"].vms / KB,
                        pinfo["memory_info"].rss / KB,
                       
                        
                        pinfo["cpu_percent"], # cpu utilization is a good secondary data point for the purpose 
                                              # of identifying possible root causes
                      
                        pinfo["cpu_times"],   # cumulative cpu utilization may help determine if the process had
                                              # a "lead-up" to failure, or rather quickly became unstable
                      
                        # the following info may be "overkill" - willing to consider leaving
                        # it out, or perhaps making it available via some optional configuration
                        pinfo["connections"], # active connections may provide information about 3rd party API Service 
                                              # calls that may be behaving unexpectedly.  For example, we occasionally 
                                              # encounter memory spikes when data is being queued for transfer to S3, 
                                              # especially during times when AWS infrastructure is degraded.
                      
                        pinfo["open_files"],  # similarly, open files may provide useful information when our parsing 
                                              # infrastructure fails.  Typically, we see this occur when an unusual / 
                                              # unexpectedly large data payload needs to be processed.   We've needed to
                                              # fix several memory leaks related to this type of scenario.
                        PROC_STATUSES_RAW.get(pinfo['status']) or "?",
                        subprocess.list2cmdline(pinfo["cmdline"])
                    ]
                    for pinfo in msg.ps_table
                ]

                bits = [
                    "container exceeded its memory allocation",
                    "container is restarting:",
                    tabulate(table_data, headers=[
                        "PID", "VSZ", "RSS", "STAT", "COMMAND"
                    ], tablefmt="plain")
                ]

                for bit in bits:
                    self._log_activity(msg.cg.name(), bit)
            elif isinstance(msg, RestartTimeoutMessage):
                m = "container did not exit within {0} seconds grace " \
                    "period".format(msg.grace_period)
                self._log_activity(msg.cg.name(), m)
            elif isinstance(msg, ExitMessage):
                logger.warning("shutting down")
                break
            else:
                raise Exception("Unexpected message: {0}".format(msg))

    def _log_activity(self, cg_name, message):
        activity_file = os.path.join(self.activity_dir,
                                     "{0}-json.log".format(cg_name))

        with open(activity_file, "a") as f:
            ts = datetime.datetime.utcnow().replace(tzinfo=Utc())
            json.dump({"log": message, "time": ts.isoformat()}, f)
            f.write("\n")

        for line in [l for l in message.split("\n") if l]:
            logger.info("%s: %s", cg_name, line)
