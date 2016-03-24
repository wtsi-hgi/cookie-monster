"""
Authors
-------
* Colin Nolan <cn13@sanger.ac.uk>

License
-------
GPLv3 or later
Copyright (c) 2016 Genome Research Limited
"""
from cookiemonster.monitor.monitor import Monitor


class StubMonitor(Monitor):
    """
    Stub `Monitor`.
    """
    def do_log_record(self):
        pass
