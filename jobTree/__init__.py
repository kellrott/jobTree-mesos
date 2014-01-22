
import sys
import os
import re
import pickle
from copy import copy

from glob import glob
import mesos
import mesos_pb2

import logging
logging.basicConfig(filename='jobTree.log',level=logging.DEBUG)


class Target(object):
    """
    The target class describes a python class to be pickel'd and 
    run as a remote process at a later time.
    """

    def __init__(self, **kwargs):
        pass

    def run(self):
        """
        The run method is user provided and run in a seperate process,
        possible on a remote node.
        """
        raise Exception()

   
    def addChildTarget(self, child):
        """
        Add child target to be executed
        
        :param child_name:
            Unique name of child to be executed
        
        :param child:
            Target object to be pickled and run as a remote 
        
        
        Example::
            
            class MyWorker(jobtree.Target):
                
                def __init__(self, dataBlock):
                    self.dataBlock = dataBlock
                
                def run(self):
                    c = MyOtherTarget(dataBlock.pass_to_child)
                    self.addChildTarget('child', c )       
        """
        self.__manager__._addTarget(child)
   
    def setFollowOnTarget(self, child):
        self.addFollowTarget(child)
        
    def addFollowTarget(self, child):
        """
        A follow target is a delayed callback, that isn't run until all 
        of a targets children have complete
        """
        self.__manager__._addFollowTarget(child)


class Stack(object):

    def __init__(self, target):
        self.target = target

    @staticmethod
    def addJobTreeOptions(parser):
        parser.add_option("--batchSystem", dest="batchSystem",
                      help="URL of Mesos Server default=%default",
                      default="localhost:5050")

        parser.add_option("--jobTree", dest="jobTree", 
                      default=".jobTree")
    
    def startJobTree(self, options):
        self.options = options
        extra_path = os.path.dirname(os.path.abspath(sys.argv[0]))
        os.environ['PYTHONPATH'] = os.environ.get('PYTHONPATH', "") + ":" + extra_path
        self.jt_manager = JobTreeManager(options.batchSystem, os.path.abspath(options.jobTree))
        self.jt_manager.queue.addTask( '/s', self.target ) 
        self.jt_manager.run()


class JobTreeManager:

    def __init__(self, mesos_url, common_dir):
        self.mesos_url = mesos_url
        self.common_dir = common_dir
        self.queue = JobTreeQueue(os.path.abspath(common_dir))
    
    """
    def addTargetList(self, targetList):
        for name, obj in targetList:
            print name, obj
    """

    def run(self):
        self.queue.branchUpdate("/")
        executor = mesos_pb2.ExecutorInfo()
        executor.executor_id.value = "default"
        executor.command.value = "PYTHONPATH=%s %s " % (os.environ['PYTHONPATH'], sys.executable) + os.path.abspath(os.path.join(os.path.dirname(__file__), "./jobTreeExec.py %s" % (self.common_dir)) )
        executor.name = "JobTreeExec"
        executor.source = "JobTree"

        framework = mesos_pb2.FrameworkInfo()
        framework.user = "" # Have Mesos fill in the current user.
        framework.name = "JobTree"

        # TODO(vinod): Make checkpointing the default when it is default
        # on the slave.
        if os.getenv("MESOS_CHECKPOINT"):
            logging.info("Enabling checkpoint for the framework")
            framework.checkpoint = True

        if os.getenv("MESOS_AUTHENTICATE"):
            logging.info("Enabling authentication for the framework")

            if not os.getenv("DEFAULT_PRINCIPAL"):
                logging.error("Expecting authentication principal in the environment")
                return 

            if not os.getenv("DEFAULT_SECRET"):
                logging.error("Expecting authentication secret in the environment")
                return

            credential = mesos_pb2.Credential()
            credential.principal = os.getenv("DEFAULT_PRINCIPAL")
            credential.secret = os.getenv("DEFAULT_SECRET")

            driver = mesos.MesosSchedulerDriver(
            JobTreeScheduler(executor, self.queue),
            framework,
            self.mesos_url,
            credential)
        else:
            logging.info("Contacting Mesos: %s" % self.mesos_url)
            driver = mesos.MesosSchedulerDriver(
                JobTreeScheduler(executor, self.queue),
                framework,
                self.mesos_url)
                
        status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1
        logging.info( "Status: %s" % status )
        # Ensure that the driver process terminates.
        driver.stop();

rePathClean = re.compile(r'^\/')
class JobTreeQueue(object):
    TASK_QUEUED = "queued"
    TASK_RUNNING = "running"
    TASK_FINISHED = "finished"
    TASK_FAILED = "failed"

    TARGET_SUFFIX = ".t"
    CHILDREN_SUFFIX = ".c"
    FOLLOW_SUFFIX = ".f"
    DONE_SUFFIX = ".d"


    def __init__(self, basedir):
        self.basedir = basedir
        self.task_tree = {}

        if not os.path.exists(self.basedir):
            os.mkdir(self.basedir)

    def branchUpdate(self, taskpath):
        logging.debug("UPDATE: %s" % taskpath)
        task_file_path = self.taskFilePath(taskpath)
        task_split = self.taskPathSplit(taskpath)

        if os.path.isdir(task_file_path):
            logging.debug("SCANNING %s" % task_file_path)
            for target_file in glob(os.path.join(task_file_path, "*%s" % (self.TARGET_SUFFIX))):
                logging.debug("TARGET_FOUND: %s" % target_file)
                target_file_base = re.sub(self.TARGET_SUFFIX + "$", "", target_file)
                task_name = os.path.basename(target_file_base)
                if not os.path.exists( target_file_base + self.DONE_SUFFIX ):
                    self._taskTreeAdd(task_split + [task_name])

            for child in glob(os.path.join(task_file_path, "*%s" % (self.CHILDREN_SUFFIX))):
                task_name = os.path.basename(child)
                self.branchUpdate(os.path.join(taskpath, task_name))

            for child in glob(os.path.join(task_file_path, "*%s" % (self.FOLLOW_SUFFIX))):
                task_name = os.path.basename(child)
                self.branchUpdate(os.path.join(taskpath, task_name))
        else:
            if os.path.exists( task_file_path + self.CHILDREN_SUFFIX ):
                self.branchUpdate( taskpath + self.CHILDREN_SUFFIX)
            if os.path.exists( task_file_path + self.FOLLOW_SUFFIX ):
                self.branchUpdate( taskpath + self.FOLLOW_SUFFIX)

        

    def taskFilePath(self, taskpath):
        return os.path.join(self.basedir, rePathClean.sub("", taskpath))

    def taskPathSplit(self, taskpath):
        c = rePathClean.sub("", taskpath)
        if len(c) == 0:
            return []
        return c.split("/")

    def addTask(self, taskpath, task):
        targetPath = self.taskFilePath(taskpath) + self.TARGET_SUFFIX
        d = pickle.dumps(task)
        handle = open(targetPath, "w")
        handle.write(d)
        handle.close()
        self._taskTreeAdd(self.taskPathSplit(taskpath))

    def setTaskRunning(self, taskpath):
        logging.info("TASK_RUNNING: %s" % (taskpath))
        p = self.taskPathSplit(taskpath)
        branch = self.task_tree
        for v in p[:-1]:
            branch = branch[v]
        branch[p[-1]] = self.TASK_RUNNING

    def setTaskFinished(self, taskpath):
        logging.info("TASK_FINISHED: %s" % (taskpath))
        targetPath = self.taskFilePath(taskpath) + self.DONE_SUFFIX
        handle = open(targetPath, "w")
        handle.close()

        p = self.taskPathSplit(taskpath)
        branch = self.task_tree
        for v in p[:-1]:
            branch = branch[v]
        branch[p[-1]] = self.TASK_FINISHED
        self._taskTreeSweep()
        logging.info( "FINISHED: %s" % self.task_tree )

    def setTaskFailed(self, taskpath):
        loggin.info("TASK_FAILED: %s" % (taskpath))
        p = self.taskPathSplit(taskpath)
        branch = self.task_tree
        for v in p[:-1]:
            branch = branch[v]
        branch[p[-1]] = self.TASK_FAILED

    def hasQueued(self, branch=None):
        if branch is None:
            branch = self.task_tree

        for key, value in branch.iteritems():
            if isinstance(value, dict):
                if self.hasQueued(value):
                    return True
            else:
                if value == self.TASK_QUEUED or value == self.TASK_RUNNING:
                    return True
        return False


    def _taskTreeAdd(self, path_split, branch=None):
        if branch is None:
            branch = self.task_tree

        if len(path_split) == 1:
            if path_split[0] not in branch:
                branch[path_split[0]] = self.TASK_QUEUED
        else:
            if path_split[0] not in branch:
                branch[path_split[0]] = {}
            self._taskTreeAdd(path_split[1:], branch[path_split[0]])

    def _taskTreeSweep(self, branch=None):
        if branch is None:
            branch = self.task_tree
        sweep = True
        sweep_list = []
        for key, value in branch.items():
            if isinstance(value, dict):
                if not self._taskTreeSweep(value):
                    sweep = False
                else:
                    sweep_list.append(key)
            else:
                if value != self.TASK_FINISHED:
                    sweep = False
        for k in sweep_list:
            branch[k] = self.TASK_FINISHED
        return sweep

    def getTask(self, active_set, path_split=None, branch=None):
        if path_split is None:
            path_split = []
        if branch is None:
            branch = self.task_tree
        for key, value in branch.iteritems():
            if isinstance(value, dict):
                if not key.endswith(self.FOLLOW_SUFFIX) or branch.get(re.sub(self.FOLLOW_SUFFIX + "$", self.CHILDREN_SUFFIX, key), self.TASK_FINISHED ) == self.TASK_FINISHED:
                    path = self.getTask( active_set, path_split + [key], value )
                    if path is not None:
                        return path
            else:
                if value == self.TASK_QUEUED:
                    path = "/" + "/".join(path_split + [key])
                    if path not in active_set:
                        return path
        return None


class JobTreeScheduler(mesos.Scheduler):
    def __init__(self, executor, queue):
        self.executor = executor
        self.queue = queue

        self.active_set = {}
        self.taskData = {}

        self.TASK_CPUS = 1
        self.TASK_MEM = 512

        self.tasksLaunched = 0
        self.tasksFinished = 0
        self.messagesSent = 0
        self.messagesReceived = 0

    def registered(self, driver, frameworkId, masterInfo):
        logging.info("Registered with framework ID %s" % frameworkId.value)

    def resourceOffers(self, driver, offers):
        logging.info("Got %d resource offers" % len(offers))
        if not self.queue.hasQueued():
            driver.stop()
            return
        for offer in offers:
            logging.info("Got resource offer %s" % offer.id.value)

            cpu_count = 1
            for res in offer.resources:
                if res.name == 'cpus':
                    cpu_count = int(res.scalar.value)

            logging.info("CPU_COUNT: %s" % cpu_count)
            tasks = []
            taskPaths = []
            tmp_active = copy(self.active_set)
            for i in range(cpu_count):
                taskPath = self.queue.getTask(tmp_active)
                if taskPath is not None:
                    tmp_active[taskPath] = True
                    taskPaths.append(taskPath)

                    logging.info("Accepting offer on %s to start task %s" % (offer.hostname, taskPath))

                    task = mesos_pb2.TaskInfo()
                    task.task_id.value = taskPath
                    task.slave_id.value = offer.slave_id.value
                    task.name = "task %s" % taskPath
                    task.executor.MergeFrom(self.executor)

                    cpus = task.resources.add()
                    cpus.name = "cpus"
                    cpus.type = mesos_pb2.Value.SCALAR
                    cpus.scalar.value = self.TASK_CPUS

                    mem = task.resources.add()
                    mem.name = "mem"
                    mem.type = mesos_pb2.Value.SCALAR
                    mem.scalar.value = self.TASK_MEM

                    self.taskData[task.task_id.value] = (offer.slave_id, task.executor.executor_id)
                    tasks.append(task)
                else:
                    logging.info("Declining Offer")
                    break                  
            status = driver.launchTasks(offer.id, tasks)
            if status == mesos_pb2.DRIVER_RUNNING:
                for taskPath in taskPaths:
                    self.active_set[taskPath] = True
            else:
                logging.error("Launch Task Fail: %s" % (status))

            logging.info("LaunchStatus: %s : %s " % (status, " ".join(taskPaths)))

    def statusUpdate(self, driver, update):
        logging.info("Task %s is in state %d" % (update.task_id.value, update.state))
        if update.state == mesos_pb2.TASK_RUNNING:
            self.queue.setTaskRunning( str(update.data) )
        if update.state == mesos_pb2.TASK_FINISHED:
            del self.active_set[str(update.data)]
            self.queue.branchUpdate(str(update.data + self.queue.CHILDREN_SUFFIX))
            self.queue.branchUpdate(str(update.data + self.queue.FOLLOW_SUFFIX))
            self.queue.setTaskFinished( str(update.data) )
        if update.state == mesos_pb2.TASK_FAILED:
            self.queue.setTaskFailed( str(update.data) )

    def frameworkMessage(self, driver, executorId, slaveId, message):
        self.messagesReceived += 1
        