#!/usr/bin/env python

import multiprocessing
import time
import sys
import os
import subprocess
import datetime
import signal

# global vars
BASE_NAME = os.path.dirname(os.path.realpath(__file__))
CALCID_EXECUTABLE = BASE_NAME + "/CalcID.py"
LOCK = multiprocessing.Lock()
ENABLE_BUSY_TIME_POLICY = True 	# enable busy time policy
WORK_HALTED = False

# run CalcID process
def run_CalcID(folder, job):
	return subprocess.call([CALCID_EXECUTABLE, folder, job])

# get jobs file and return a list of jobs
def get_jobs(ids_file):
	jobs = [line.rstrip('\n') for line in open(ids_file)]
	jobs = filter(None, jobs) 	# remove empty lines
	return jobs

# remove a job from file
def remove_job(job, ids_file):
	f = open(ids_file,"r+")
	d = f.readlines()
	f.seek(0)
	for i in d:
		if i.strip('\n') != job:
			f.write(i)
	f.truncate()
	f.close()

# main worker code. get queue of jobs and prcoess them
def worker_main(queue):
    print "parallel:\tworker",os.getpid(),"created"
    while True:
        job = queue.get(True)
        print "parallel:\tworker",os.getpid(), "got job id", job
	ret_val = run_CalcID(folder, job)
	if ret_val != 0:
    		print "parallel:\tenqueueing",job,"again after a failed attempt to run"
		the_queue.put(job)
	else:
		# using lock to ensure file operation is atomic
		LOCK.acquire() 
		remove_job(job, ids_file)
		print "parallel:\tworker",os.getpid(),"removed",job,"from",ids_file
		LOCK.release()

# enforce busy time policy
def busy_time_policy():
	global WORK_HALTED

	if is_busy_time(datetime.datetime.now()):
		if WORK_HALTED == False:
			print "parallel:\tbusy time: halting work"
			halt_work()
			WORK_HALTED = True
	else:
		if WORK_HALTED == True:
			print "parallel:\tbusy time over: continuing work"
			unhalt_work()
			WORK_HALTED = False
	

# sends a signal to a process based on name
def pkill(name,signal):
	p = subprocess.Popen(['ps', '-ef'], stdout=subprocess.PIPE)
        out, err = p.communicate()
        for line in out.splitlines():
        	if name in line:
                	pid = int(line.split()[1])
                        os.kill(pid, signal)

# returns whether time is on busy hours plan
def is_busy_time(time):
	minute = time.minute
        if 10 <= minute % 15 <= 14 or minute % 15 == 0:
		return True
	else:
		return False

# halt work
def halt_work():
       	pkill('CalcID', signal.SIGTSTP)

# continue work
def unhalt_work():
	pkill('CalcID', signal.SIGCONT)

# check args
def check_args(args):
	if len(args) != 4:
		print "parallel.py expects 3 arguments."
		print "arg1: folder name"
		print "arg2: path to ids file"
		print "arg3: maximum number of processes to run in parallel"
		sys.exit(1)

	if not os.path.isfile(args[2]):
		print args[2] + " doesn't exists."
		sys.exit(1)
		

	try:
		val = int(args[3])
	except ValueError:
		print(args[3] + "is not an integer")

# main
if __name__ == '__main__':
	# check vars
	check_args(sys.argv)
	
	# set vars
	folder = sys.argv[1]
	ids_file = sys.argv[2]
	max_parallel_processes = int(sys.argv[3])	
	the_queue = multiprocessing.Queue()

	# start pool of jobs and workers
	the_pool = multiprocessing.Pool(max_parallel_processes, worker_main,(the_queue,))

	# get jobs and add them to queue
	jobs = get_jobs(ids_file)
	for job in jobs:
		the_queue.put(job)

	# work tile ids_file is empty
	while os.stat(ids_file).st_size != 0:
		time.sleep(1)
	        if ENABLE_BUSY_TIME_POLICY:
			busy_time_policy()

	# finish and exit
	time.sleep(1)
	print("\nno more jobs to process")
