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
from socket import *
from pika.exceptions import AMQPConnectionError

import metafeedertask
import subprocess
import sys
import time
import os
import shlex
import re
import misc
import thread


# Global Variables
migrator = []
validator = []
metafeeder = []
basepath = ''

def report_error(message):
	errormsg = "NFSToManager report error: " + message
	misc.report_error(errormsg, False)

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
	return "Migrator: " + repr(migarr)

def spawn_validator(number=2):
	global validator
	adjust_proc(number, validator, "validatortask.py")

def adjust_proc(number, procs, script):
	global basepath

	current = len(procs)
	if (current > number):
		delta = current - number
		for i in range(0, delta):
			procs[i].kill()
			procs[i].wait()
		del procs[0:delta]
	elif (current < number):
		command = "python " + os.path.join(basepath, script)
		print "Execute: " + command
		for i in range(current, number):
			process = subprocess.Popen(shlex.split(command))
			procs.append(process)

def list_validator():
	valarr = []
	for proc in validator:
		valarr.append(proc.pid)
	return "Validator: " + repr(valarr)

def reset_migrator():
	length = len(migrator)

	kill_migrator()
	spawn_migrator(length)

def reset_validator():
	length = len(validator)
	
	kill_validator()
	spawn_validator(length)

def kill_migrator():
	global migrator

        for proc in migrator:
                proc.kill()
                proc.wait()
        del migrator[:]

def kill_validator():
	global validator

 
	for proc in validator:
  		proc.kill()
                proc.wait()
	del validator[:]

def kill_all():
	kill_migrator()
	kill_validator()

def count_validation_job():
	return "Validation job: " + str(validationmq.get_message_count())

def count_migration_job():
	return "Migration job: " + str(migrationmq.get_message_count())

def count_feeder_job():
	return "Feeder job: " + str(feedermq.get_message_count())

def do_command(usercmd):
	global cont_flag
	global validationmq
	global migrationmq
	message = 'ok\n'

	if usercmd == "!count_migration_job":
		message = count_migration_job()
	elif usercmd == "!count_validation_job":
		message = count_validation_job()
	elif usercmd == "!help":
		message = help()
	elif (usercmd == "!kill_all"):
		kill_all()
	elif (usercmd == "!list_migrator"):
		message = list_migrator()
	elif (usercmd == "!list_validator"):
		message = list_validator()
	elif (usercmd == "!reset_migrator"):
		reset_migrator()
	elif (usercmd == "!reset_validator"):
		reset_validator()
	elif usercmd == "!start":
		start()
	elif usercmd == "!stats":
		message = stats()
	elif (usercmd == "!quit"):
		kill_all()
		cont_flag = False
	else:
		message = do_regex_match(usercmd)
	return message + '\n'

def do_regex_match(usercmd):
	global feeder_basepath
	message = 'ok'

	match_obj = re.match(r'!want_migrator (\d+)', usercmd, re.I)
	if match_obj:
		procnumber = misc.to_numeric(match_obj.group(1), 2)
		spawn_migrator(procnumber)
	else:
		match_obj = re.match(r'!want_validator (\d+)', usercmd, re.I)
		if match_obj:
			procnumber = misc.to_numeric(match_obj.group(1), 2)
			spawn_validator(procnumber)
		else:
			message = "Unknown command: " + usercmd
			message += "\nType !help for help"
	return message

def start():
	spawn_migrator()
	spawn_validator()

def stats():
	global feedermq

	message = ''
	message += list_migrator()
	message += '\n' + count_migration_job()
	message += '\n' + list_validator()
	message += '\n' + count_validation_job()
	return message + '\n'

def help():
	message = '\n'
	message += "\nAvailable command:"
	message += "\nclose"
	message += "\n!count_migration_job"
	message += "\n!count_validation_job"
	message += "\n!help"
	message += "\n!kill_all"
	message += "\n!list_migrator"
	message += "\n!list_validator"
	message += "\n!quit"
	message += "\n!reset_migrator"
	message += "\n!reset_validator"
	message += "\n!start"
	message += "\n!stats"
	message += "\n!want_migrator [number of migrator]"
	message += "\n!want_validator [number of validator]"
	return message

def remote_handler(sock, addr):
	try:
		global migrationmq
		global validationmq
		global feedermq
		
		migrationmq = RMQClient(queue="migration_job_queue")
		validationmq = RMQClient(queue="validation_job_queue")
		feedermq = RMQClient(queue="directory_job_queue")
		while 1:
			data = sock.recv(1024)
			if not data: break
			message = data.strip()
			if "close" == data.strip():
				break
			else:
				result = do_command(message)
				sock.send(result)
	except AMQPConnectionError:
		report_error('rabbitmq')
		sock.close()
	finally:
		sock.close()
		print addr, "- closed connection"

if __name__ == "__main__":
	setup()

	ADDR = ("0.0.0.0", 12012)
	serversock = socket(AF_INET, SOCK_STREAM)
	serversock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
	serversock.bind(ADDR)
	serversock.listen(5)
	print "Listen on 0.0.0. port 12012"
	while 1:
		clientsock, addr = serversock.accept()
		thread.start_new_thread(remote_handler, (clientsock, addr))
