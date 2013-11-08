#!/usr/bin/env python

import sys
import os
import threading
import time
import re
import pickle

import mesos
import mesos_pb2

rePathClean = re.compile(r'^\/')

def log(m):
    sys.stderr.write(m + "\n")

NAME_BASE = "c_"
TARGET_SUFFIX = ".t"
CHILDREN_SUFFIX = ".c"
FOLLOW_SUFFIX = ".f"

class TargetManager(object):
    def __init__(self):
        self.child_count = 0
        self.child_list = []
        self.follow_list = []

    def _addTarget(self, target):
        name = "%s%d" % (NAME_BASE, self.child_count)
        self.child_count += 1
        self.child_list.append( (name, target) )

    def _addFollowTarget(self, target):
        name = "%s%d" % (NAME_BASE, self.child_count)
        self.child_count += 1
        self.follow_list.append( (name, target) )


class MyExecutor(mesos.Executor):
    def __init__(self, basedir):
        self.basedir = basedir
    def launchTask(self, driver, task):
        # Create a thread to run the task. Tasks should always be run in new
        # threads or processes, rather than inside launchTask itself.
        def run_task():
            task_path =  task.task_id.value
            log("Running task %s" % (task_path))

            work_path = os.path.join(self.basedir, rePathClean.sub("", task_path))

            update = mesos_pb2.TaskStatus()
            update.task_id.value = task_path
            update.state = mesos_pb2.TASK_RUNNING
            update.data = str(task_path)
            driver.sendStatusUpdate(update)

            fail = True
            try:
                handle = open(work_path + TARGET_SUFFIX)
                obj = pickle.loads(handle.read())
                handle.close()
                obj.__manager__ = TargetManager()
                obj.run()

                if len(obj.__manager__.child_list):
                    child_dir = os.path.join(work_path + CHILDREN_SUFFIX)
                    if not os.path.exists(child_dir):
                        os.mkdir(child_dir)
                    for name, target in obj.__manager__.child_list:
                        target_path = os.path.join(child_dir, name + TARGET_SUFFIX)
                        handle = open(target_path, "w")
                        handle.write(pickle.dumps(target))
                        handle.close()

                if len(obj.__manager__.follow_list):
                    follow_dir = os.path.join(work_path + FOLLOW_SUFFIX)
                    if not os.path.exists(follow_dir):
                        os.mkdir(follow_dir)
                    for name, target in obj.__manager__.follow_list:
                        target_path = os.path.join(follow_dir, name + TARGET_SUFFIX)
                        handle = open(target_path, "w")
                        handle.write(pickle.dumps(target))
                        handle.close()

                fail = False
            except Exception, e:
                log(str(e))

            log("Sending status update...")
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task_path
            if fail:
                update.state = mesos_pb2.TASK_FAILED
            else:
                update.state = mesos_pb2.TASK_FINISHED
            update.data = str(task_path)
            driver.sendStatusUpdate(update)
            log("Sent status update")

        thread = threading.Thread(target=run_task)
        thread.start()

    def frameworkMessage(self, driver, message):
        # Send it back to the scheduler.
        driver.sendFrameworkMessage(message)

if __name__ == "__main__":
    print "Starting executor"
    driver = mesos.MesosExecutorDriver(MyExecutor(sys.argv[1]))
    sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)