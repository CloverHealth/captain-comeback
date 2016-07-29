# coding:utf-8


# TODO: We should pass a read-only view of the cgroup instead to ensure
# thread-safety.
class NewCgroupMessage(object):
    def __init__(self, cg):
        self.cg = cg


class StaleCgroupMessage(object):
    def __init__(self, cg):
        self.cg = cg


class RestartCgroupMessage(object):
    def __init__(self, cg, ps_table):
        self.cg = cg
        self.ps_table = ps_table


class ExitMessage(object):
    pass
