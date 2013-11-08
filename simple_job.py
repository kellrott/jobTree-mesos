#!/usr/bin/env python

import time

from jobTree import Target

from jobTree.scriptTree.target import Target
from jobTree.scriptTree.stack import Stack

from optparse import OptionParser


class First(Target):
    def __init__(self, value):
        self.value = value

    def run(self):
        print "The Target has run:", self.value
        self.addChildTarget(Second("I'm almost ready"))
        self.addChildTarget(Second("I'm ready"))
        self.setFollowOnTarget(Final("I'm Done"))

class Second(Target):
    def __init__(self, value):
        self.value = value

    def run(self):
        time.sleep(5)
        print "The Second Target has run:", self.value

class Final(Target):
    def __init__(self, value):
        self.value = value

    def run(self):
        print "The Second Target has run:", self.value


if __name__ == "__main__":
    import simple_job
    parser = OptionParser()
    Stack.addJobTreeOptions(parser) 

    options, args = parser.parse_args()
    
    i = Stack(simple_job.First("hello world")).startJobTree(options)