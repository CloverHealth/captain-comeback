# coding:utf-8
import os
import time
import pwd
import grp
import subprocess
import unittest
import uuid
import resource
from six.moves import queue

from captain_comeback.index import CgroupIndex
from captain_comeback.cgroup import Cgroup
from captain_comeback.restart.messages import RestartRequestedMessage
from captain_comeback.activity.messages import (NewCgroupMessage,
                                                StaleCgroupMessage)
from captain_comeback.activity.status import PROC_STATUSES_RAW

from captain_comeback.test.queue_assertion_helper import (
        QueueAssertionHelper)


CG_PARENT_NAME = "captain-comeback-integration"
CG_ROOT_DIR = "/sys/fs/cgroup/memory"


def descriptor_from_cg_path(path):
    return "memory:{0}".format(path.replace(CG_ROOT_DIR, ""))


def delete_cg(path, recursive=False):
    command = ["sudo", "cgdelete", "-g", descriptor_from_cg_path(path)]
    if recursive:
        command.append("-r")
    subprocess.call(command)


def create_cg(name, parent_path=None):
    parent_path = parent_path or CG_ROOT_DIR
    path = "{0}/{1}".format(parent_path, name)

    user_name = pwd.getpwuid(os.geteuid()).pw_name
    group_name = grp.getgrgid(os.getegid()).gr_name
    user_spec = "{0}:{1}".format(user_name, group_name)

    subprocess.check_call(["sudo", "cgcreate", "-g",
                           descriptor_from_cg_path(path),
                           "-t", user_spec, "-a", user_spec])

    return path


def create_random_cg(parent_path=None):
    return create_cg(str(uuid.uuid4()), parent_path)


def enable_memlimit_and_trigger_oom(path):
    cg = Cgroup(path)
    cg.open()

    # Set a memory limit then disable the OOM killer via a wakeup
    cg.set_memory_limit_in_bytes(1024 * 1024 * 128)  # 128 MB
    cg.wakeup(queue.Queue())
    cg.close()

    test_program = 'l = []\nwhile True:\n  l.append(object())'

    subprocess.Popen(["sudo", "cgexec", "-g", descriptor_from_cg_path(path),
                      "python", "-c", test_program])


class CgroupTestIntegration(unittest.TestCase, QueueAssertionHelper):
    def setUp(self):
        self.parent_cg_path = create_cg(CG_PARENT_NAME)

    def tearDown(self):
        # Kill every task in subgroups to be safe, then recursively tear down
        # everything.
        try:
            list_pids_cmd = "cat {0}/*/tasks".format(self.parent_cg_path)
            tasks = subprocess.check_output(list_pids_cmd, shell=True,
                                            stderr=subprocess.PIPE).split()
        except subprocess.CalledProcessError:
            # No tasks
            pass
        else:
            for task in tasks:
                subprocess.check_call(["sudo", "kill", "-KILL", task])

        delete_cg(self.parent_cg_path, recursive=True)

    def test_index_sync(self):
        cg_path = create_random_cg(self.parent_cg_path)

        q = queue.Queue()
        index = CgroupIndex(self.parent_cg_path, q, q)
        index.open()
        index.sync()

        # Check that the CG was added to the path hash
        self.assertEqual(1, len(index._path_hash))
        self.assertEqual(1, len(index._efd_hash))

        # Check that the CG was registered (adding it again will cause an
        # error)
        cg = index._path_hash[cg_path]
        self.assertRaises(EnvironmentError, index.epl.register,
                          cg.event_fileno())

        index.close()

    def test_index_sync_many(self):
        cg_paths = [create_random_cg(self.parent_cg_path) for _ in range(10)]

        q = queue.Queue()
        index = CgroupIndex(self.parent_cg_path, q, q)
        index.open()
        index.sync()

        while cg_paths:
            self.assertEqual(len(cg_paths), len(index._path_hash))

            path = cg_paths.pop()
            self.assertIn(path, index._path_hash)

            delete_cg(path)
            index.sync()

            self.assertEqual(len(cg_paths), len(index._path_hash))
            self.assertNotIn(path, index._path_hash)

        index.close()

    def test_wakeup_on_sync(self):
        cg_path = create_random_cg(self.parent_cg_path)

        cg = Cgroup(cg_path)
        cg.open()
        cg.set_memory_limit_in_bytes(1024)
        self.assertEqual("0", cg.oom_control_status()["oom_kill_disable"])

        q = queue.Queue()
        index = CgroupIndex(self.parent_cg_path, q, q)
        index.open()
        index.sync()
        index.close()

        self.assertEqual("1", cg.oom_control_status()["oom_kill_disable"])
        cg.close()

    def test_index_poll(self):
        cg_path = create_random_cg(self.parent_cg_path)

        job_q = queue.Queue()
        activity_q = queue.Queue()
        index = CgroupIndex(self.parent_cg_path, job_q, activity_q)
        index.open()

        self.assertHasNoMessages(activity_q)
        index.sync()

        enable_memlimit_and_trigger_oom(cg_path)
        self.assertHasNoMessages(job_q)
        index.poll(10)

        self.assertHasMessageForCg(job_q, RestartRequestedMessage, cg_path)
        self.assertHasMessageForCg(activity_q, NewCgroupMessage, cg_path)

        index.close()

    def test_index_poll_many(self):
        for _ in range(10):
            create_random_cg(self.parent_cg_path)
        cg_path = create_random_cg(self.parent_cg_path)

        job_q = queue.Queue()
        activity_q = queue.Queue()
        index = CgroupIndex(self.parent_cg_path, job_q, activity_q)
        index.open()

        self.assertHasNoMessages(activity_q)
        index.sync()
        for _ in range(11):
            self.assertHasMessageForCg(activity_q, NewCgroupMessage,
                                       self.ANY_CG)
        self.assertHasNoMessages(activity_q)

        enable_memlimit_and_trigger_oom(cg_path)
        self.assertHasNoMessages(job_q)
        index.poll(10)

        self.assertHasMessageForCg(job_q, RestartRequestedMessage, cg_path)

        index.close()

    def test_index_poll_close(self):
        cg_path = create_random_cg(self.parent_cg_path)

        job_q = queue.Queue()
        activity_q = queue.Queue()
        index = CgroupIndex(self.parent_cg_path, job_q, activity_q)
        index.open()

        self.assertHasNoMessages(activity_q)
        index.sync()
        self.assertHasMessageForCg(activity_q, NewCgroupMessage, cg_path)

        delete_cg(cg_path)
        index.sync()
        self.assertHasMessageForCg(activity_q, StaleCgroupMessage, cg_path)

        index.close()

    def test_open_close(self):
        cg_path = create_random_cg(self.parent_cg_path)

        cg = Cgroup(cg_path)
        cg.open()
        cg.close()

    def test_set_memory_limit(self):
        cg_path = create_random_cg(self.parent_cg_path)

        # Memory limits are enforced as a page size count, so we have to make
        # sure we choose a number that's properly aligned.
        limit = 123 * resource.getpagesize()
        cg = Cgroup(cg_path)
        cg.set_memory_limit_in_bytes(limit)
        self.assertEqual(limit, cg.memory_limit_in_bytes())

    def test_disable_oom_killer(self):
        cg_path = create_random_cg(self.parent_cg_path)

        cg = Cgroup(cg_path)
        cg.open()
        cg.wakeup(queue.Queue())
        self.assertEqual("0", cg.oom_control_status()["oom_kill_disable"])

        # The OOM Killer should be disabled if there is a task limit
        cg.set_memory_limit_in_bytes(1024)
        cg.wakeup(queue.Queue())
        self.assertEqual("1", cg.oom_control_status()["oom_kill_disable"])

        cg.close()

    def test_stale_cgroup(self):
        cg_path = create_random_cg(self.parent_cg_path)

        cg = Cgroup(cg_path)
        cg.open()

        delete_cg(cg_path)

        q = queue.Queue()
        cg.wakeup(q)
        self.assertRaises(EnvironmentError, cg.wakeup, q, raise_for_stale=True)

        cg.close()

    def test_reopen(self):
        cg_path = create_random_cg(self.parent_cg_path)

        cg = Cgroup(cg_path)
        cg.open()
        self.assertRaises(AssertionError, cg.open)
        cg.close()

    def test_reclose(self):
        cg_path = create_random_cg(self.parent_cg_path)

        cg = Cgroup(cg_path)
        cg.open()
        cg.close()
        self.assertRaises(AssertionError, cg.close)

    def test_trigger_restart(self):
        cg_path = create_random_cg(self.parent_cg_path)

        q = queue.Queue()
        cg = Cgroup(cg_path)
        cg.open()

        cg.wakeup(q)
        self.assertHasNoMessages(q)

        enable_memlimit_and_trigger_oom(cg_path)

        # The test program should fill 128 MB rather fast; give it 10s
        for _ in range(100):
            cg.wakeup(q)
            try:
                msg = q.get_nowait()
            except queue.Empty:
                time.sleep(0.1)
                continue
            self.assertIsInstance(msg, RestartRequestedMessage)
            self.assertEqual(cg, msg.cg)
            break
        else:
            raise Exception("Queue never received a message!")

        cg.close()

    def test_ps_table(self):
        cg_path = create_random_cg(self.parent_cg_path)
        subprocess.Popen(["sudo", "cgexec", "-g",
                          descriptor_from_cg_path(cg_path),
                          "sh", "-c", "sleep 10"])
        time.sleep(2)  # Sleep for a little bit to let them spawn
        cg = Cgroup(cg_path)
        table = cg.ps_table()

        self.assertEqual(2, len(table))
        by_name = {proc["name"]: proc for proc in table}
        self.assertEqual(["sh", "sleep"], sorted(by_name.keys()))

        for name in ["sh", "sleep"]:
            proc = by_name[name]
            self.assertIsInstance(proc["pid"], int)
            self.assertIsInstance(proc["memory_info"].vms, int)
            self.assertIsInstance(proc["memory_info"].rss, int)
            self.assertIsInstance(proc["cmdline"], list)
            self.assertIn(proc["status"], PROC_STATUSES_RAW.keys())
