#!/usr/bin/env python

"""Read content of directory (Non-recursive) and then
feed the result to message queue
"""

from writer import RMQClient
from pika.exceptions import AMQPConnectionError

import reader
import sys
import time
import json
import misc
import re
import threading
import thread

job_treshold = 0
sleep_time = misc.sleep_time
directory_queue = set()
basepath = None
current_level = ''
this_task = True


def count_pending_job():
	return len(directory_queue)

def set_basepath():
	global basepath
	
	if basepath is None:
		basepath = sys.argv[1]

def add_dirs(dirs):
	global directory_queue

	for folder in dirs:
		directory_queue.add(folder['path'])
	if directory_queue:
		try:
			path = None
			directorymq = RMQClient(queue="directory_job_queue")
			while directory_queue:
				path = directory_queue.pop()
				directorymq.send(path)
		except AMQPConnectionError:
			if path is not None: directory_queue.add(path)

def get_dir():
	global directory_queue
	path = False

	try:
		directorymq = RMQClient(queue="directory_job_queue")
		path = directorymq.get()
	except AMQPConnectionError:
		pass
	if not path:
		try:
			path = directory_queue.pop()
		except KeyError:
			path = False
	return path

def send_migration_job(jobs):
	global migrationmq

	for job in jobs:
		migrationmq.send(json.dumps(job))

def execute_level(path):
	try:
		level = reader.readLevel(basepath, path)
		files = level['files']
		dirs = level['directories']
		add_dirs(dirs)
		send_migration_job(files)
	except AMQPConnectionError:
		add_dirs([path])

def control_migration_rate():
	global migrationmq
	global job_treshold
	global current_level
	global this_task
	global sleep_time

	while (this_task):
		if (migrationmq.get_message_count() > job_treshold):
			time.sleep(sleep_time)
		else:
			path = get_dir()
			if path:
				current_level = path
				execute_level(current_level)

def report_error(message):
	errormsg = "Metafeeder report component error: " + message
	misc.report_error(errormsg)

def control_job():
	global this_task
	global migrationmq
	global directorymq
	global current_level

	while (this_task):
		try:
			migrationmq = RMQClient(queue="migration_job_queue")
			execute_level(current_level)
			control_migration_rate()
		except AMQPConnectionError:
			report_error('rabbitmq')

def management_job():
	this_job = True
	while this_job:
		try:
			cmd = raw_input()
			result = do_command(cmd)
			print result
			sys.stdout.flush()
		except EOFError:
			raise

def get_help():
	message = "Available commands:"
	message += "\n!get_job_treshold"
	message += "\n!get_sleep_time"
	message += "\n!get_pending_job"
	message += "\n!job_treshold [number]"
	message += "\n!sleep_time [number]"
	return message

def do_command(cmd):
	global job_treshold
	global sleep_time
	message = 'ok'

	if cmd == "!get_job_treshold":
		message = "Job treshold: " + repr(job_treshold)
	elif cmd == "!get_sleep_time":
		message = "Sleep time: " + repr(sleep_time)
	elif cmd == "!help":
		message = get_help()
	elif cmd == "!get_pending_job":
		message = "Pending job: " + repr(directory_queue)
	else:
		match_obj = re.match(r"!job_treshold (\d+)", cmd, re.I)
		if match_obj:
			job_treshold = misc.to_numeric(match_obj.group(1), 10)
		else:
			match_obj = re.match(r'!sleep_time (\d+)', cmd, re.I)
			if match_obj:
				sleep_time = misc.to_numeric(match_obj.group(1), 10)
			else:
				message = "Unknown command\nType !help for help"
	return message

if __name__ == "__main__":
	set_basepath()
	thread.start_new_thread(control_job, ())
	management_job()
	