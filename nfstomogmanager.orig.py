#!/usr/bin/env python

"""Managing migration file from nfs to mogilefs
Migration process involve others entities such as
Python task:
	-) MigrationTask
	-) ValidationTask

Third Party System:
	-) RabbitMQ
	-) MongoDB

Assumption:
	-) Third party system is running
	-) File on NFS server is loaded on local machine
Migration process takes the following flow
-) Spawn 2 validation and migration task processes
-) Read directory and send file to migration task queue and directories to reading task queue
"""

from writer import RMQClient

import subprocess
import sys
import time
import os
import shlex
import re
import misc
import thread
from socket import *

# Global Variables
migrator = []
validator = []
basepath = ''
validationmq = RMQClient(queue="validation_job_queue")
migrationmq = RMQClient(queue="migration_job_queue")

def setup():
	global basepath
	realpath = os.path.realpath(__file__)
	basepath = os.path.dirname(realpath)

def spawn_migrator(number=2):
	global migrator
	adjust_proc(number, migrator, "migratortask.py")

def list_migrator():
	migarr = []
	for proc in migrator:
		migarr.append(proc.pid)
	print "Migrator: %s" % (migarr) 

def spawn_validator(number=2):
	global validator
	adjust_proc(number, validator, "validatortask.py")

def adjust_proc(number, procs, script):
	current = len(procs)
	if (current > number):
		delta = current - number
		for i in range(0, delta):
			procs[i].kill()
			procs[i].wait()
		del procs[0:delta]
	elif (current < number):
		command = "python " + os.path.join(basepath, script)
		for i in range(current, number):
			process = subprocess.Popen(shlex.split(command))
			procs.append(process)

def list_validator():
	valarr = []
	for proc in validator:
		valarr.append(proc.pid)
	print "Validator: %s" % (valarr)

def kill_all():
	global migrator
	global validator

	for proc in migrator:
		proc.kill()
		proc.wait()
	for proc in validator:
		proc.kill()
		proc.wait()
	del migrator[:]
	del validator[:]

def do_command(usercmd):
	global cont_flag
	global validationmq
	global migrationmq

	if usercmd == "!count_migration_job":
		print "Migration job: " + str(migrationmq.get_message_count())
	elif usercmd == "!count_validation_job":
		print "Validation job: " + str(validationmq.get_message_count())
	elif usercmd == "!help":
		help()
	elif (usercmd == "!kill_all"):
		kill_all()
	elif (usercmd == "!list_migrator"):
		list_migrator()
	elif (usercmd == "!list_validator"):
		list_validator()
	elif usercmd == "!start":
		start()
	elif (usercmd == "!quit"):
		kill_all()
		cont_flag = False
	elif (usercmd == "!count_validation"):
		print "Validatin jobs: %s" % (validationmq.get_message_count())
	elif (usercmd == "!count_migration"):
		print "Migration jobs: %s" % (migrationmq.get_message_count())
	else:
		match_obj = re.match(r'!want_migrator (\d)', usercmd, re.I)
		if match_obj:
			procnumber = misc.to_numeric(match_obj.group(1), 2)
			spawn_migrator(procnumber)
		else:
			match_obj = re.match(r'!want_validator (\d)', usercmd, re.I)
			if match_obj:
				procnumber = misc.to_numeric(match_obj.group(1), 2)
				spawn_validator(procnumber)
			else:
				print "Unknown command: " + usercmd
				help()

def start():
	spawn_migrator()
	spawn_validator()

def help():
	print "Available command"
	print "!count_migration"
	print "!count_validation"
	print "!count_migration_job"
	print "!count_validation_job"
	print "!help"
	print "!kill_all"
	print "!list_migrator"
	print "!list_validator"
	print "!quit"
	print "!start"
	print "!want_migrator [number of migrator]"
	print "!want_validator [number of validator]"

def remote_handler(sock, addr):
	while 1:
		data = sock.recv(1024)
		if not data: break
		print repr(addr) + ' recv: ' + repr(data)
		sock.sent('test')
		if "close" == data.strip(): break
	sock.close()
	print addr, "- closed connection"

if __name__ == "__main__":
	#setup()
	#cont_flag = True
	#while (cont_flag):
	#	try:
	#		usercmd = raw_input('--> ')
	#		do_command(usercmd)
	#	except IOError:
	#		time.sleep(10)
	#		pass

	ADDR = ("0.0.0.0", 12012)
	serversock = socket(AF_INET, SOCK_STREAM)
	serversock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
	serversock.bind(ADDR)
	serversock.listen(5)
	while 1:
		clientsock, addr = serversock.accept()
		thread.start_new_thread(remote_handler, (clientsock, addr))
