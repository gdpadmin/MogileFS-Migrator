#!/usr/bin/env python
"""Python task to move file from NFS to MogileFS
Flow from NFS to MogileFS are the following
1. File meta data read from message queue
2. Read file from NFS
3. Scan with Clamd file 
4. Upload to MogileFS
5. Submit validation job to message queue
"""

from mogilelogger import MogileLogger
from mogilelogger import FileLogger
from clamdscanner import ClamdScanner
from writer import RMQClient
from mogtransport import MogTransport
from fileinfo import FileInfo
from pymongo import errors
from pika.exceptions import AMQPConnectionError

import sys
import json
import string
import misc
import os
import migconfig

callback_flag = True

def report_error(message):
	global vclient
	global mqclient
	global procid

	errormsg = procid + " Migrator task report error: " + message
	misc.report_error(errormsg)

def callback(ch, method, properties, body):
	global callback_flag
	global file_logger
	global logger
	global scanner
	global trans
	global vclient
	global mqclient
	global procid
	stats = "NOT OK"

	file_logger.info(procid + " Start Migration: " + repr(body))
	meta = json.loads(body)
	key = meta["path"]
	info = FileInfo(meta["base"], key)
	fullpath = info.get_absolute_path()
	if trans.key_exist(key=key):
		file_logger.warning(procid + " Key exist: " + key)
	else:
		file_logger.info("Scanning file: " + fullpath)
		scan_result = scanner.scan_file(fullpath)
		file_logger.info(procid + " Scanned file {0}: {1}".format(fullpath,scan_result))
		if scan_result:
			trans_result = trans.send_file(source=fullpath, key=key, clas=migconfig.clas)
			message = procid + " MogileFS key {0}: {1}".format(key, trans_result)
			file_logger.info(message)
			if trans_result == True:
				coll = info.to_collection()
				logger.file_saved(coll)
				file_logger.info(procid + " Saved metadata: " + repr(coll))
				vclient.send(coll["_id"])
				file_logger.info(procid + " Validation task: " + coll["_id"])
				stats = "OK"
			elif trans_result == None:
				file_logger.warning(procid + " Not saved because key exist: " + key)
			else:
				message = procid + " Error sent file to MogileFS: " + info.to_string()
				file_logger.error(message)
				mqclient.send(body)
		else:
			coll = info.to_collection()
			coll['status'] = 'infected'
			logger.file_saved(coll)
			file_logger.error(procid + " Infected file: " + repr(coll))
	ch.basic_ack(delivery_tag = method.delivery_tag)
	file_logger.info(procid + " End migration %s: %s " %(repr(body),  stats))

if __name__ == "__main__":
	procid = misc.generate_id()
	domain = migconfig.domain
	trackers = migconfig.trackers
	file_logger = FileLogger()
	migrator_task = True
	#if len(sys.argv) == 3:
	#	domain = sys.argv[1] if sys.argv[1] else None
	#	listoftracker = sys.argv[2] if sys.argv[2] else None
	#	trackers = listoftracker.split(";")
	while migrator_task:
		try:
			file_logger.info("Initializing mogilefs client, domain %s, trackers %s" % (domain, trackers));
			trans = MogTransport(domain=domain, trackers=trackers)
			vclient = RMQClient(queue=migconfig.validation_queue)
			mqclient = RMQClient(queue=migconfig.migration_queue)
			logger = MogileLogger()
			scanner = ClamdScanner()
			scanner.init_clamd()

			mqclient.receive(callback)
		except errors.ConnectionFailure:
			report_error('mongodb')
		except errors.AutoReconnect:
			report_error('mongodb')
		except AMQPConnectionError:
			report_error('rabbitmq')
