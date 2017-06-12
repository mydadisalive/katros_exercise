#!/usr/bin/env python

import sys
import os
import time
from random import randint

MAX_SLEEP = 30 		# max time in seconds for job to finish
SUCCESS_RATE = 80
EXIT_SUCCESS = 0
EXIT_ERROR = 1 

# simulate a success or failure based or a random number between 1 and 100
def SuccessOrFailure():
	val = randint(1,100)
	if (val <= SUCCESS_RATE):
		return "success"
	else:
		return "failure"

# simulate doing something by sleeping for random number of seconds
def DoSomething(id):
	delay = randint(1,MAX_SLEEP)
	print "job " + id + ":\tdoing something (this may take few seconds to finish)"
	time.sleep(delay)
	return delay

# create a directory
def CreateDir(folder,id):
	if not os.path.exists(folder + "/" + id):
		os.makedirs(folder + "/" + id)

# perform a job with id and create a folder upon success
def CalcID(folder, id):
	print "job " + id + ":\tstarted"
	exec_time = DoSomething(id)
	status = SuccessOrFailure()
	print "job " + id + ":\tfinished. (execution time=" + str(exec_time) + "s, status=" + status + ")"
	if status == "success":
		CreateDir(folder,id)
		sys.exit(EXIT_SUCCESS)
	else:
		sys.exit(EXIT_ERROR)

# check for args validity
def CheckArgs(args):
	if len(sys.argv) != 3:
		print "CalcID expects two arguments, a directory and a job id"
		sys.exit(1)
	
# main program
if __name__ == '__main__':
	CheckArgs(sys.argv)
	CalcID(sys.argv[1], sys.argv[2])
