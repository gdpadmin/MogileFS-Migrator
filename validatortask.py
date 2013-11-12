#!/usr/bin/env python

"""Task to validate uploaded image to mogilefs
The following is validation steps
-) Receive file id from message queue
-) Get meta data of this file id from mongodb
-) Download file from mogilefs based on given meta data
-) Calculate file sha1 and size
-) Compare those meta data.
	If equal mark meta data as validated
	else remove this meta data entry from mongodb
		and send new task to migration task queue
"""

from mogilelogger import MogileLogger
from mogilelogger import FileLogger
from clamdscanner import ClamdScanner
from writer import RMQClient
from mogtransport import MogTransport
from fileinfo import FileInfo
from pymongo import errors
from pika.exceptions import AMQPConnectionError

import os
import sys
import json
import misc
import migconfig

def report_error(message):
	global vclient
	global mqclient
	global procid

	vclient.close()
	mqclient.close()
	errormsg = procid + " Validator task report error: " + message
	misc.report_error(errormsg)

def callback(ch, method, properties, body):
	stats = "OK"
	global procid

	file_logger.info(procid + " Start validation: " + repr(body))
	dbmeta = logger.file_get(body)
	file_logger.info(procid + " Database meta data: " + repr(dbmeta))
	base = "/tmp"
	name = os.path.basename(dbmeta["path"])
	fullpath = os.path.join(base, name)
	trans.download_file(key=dbmeta["path"], name=fullpath)
	mogmeta = FileInfo("/tmp", name)
	file_logger.info(procid + " MogileFS meta data: " + repr(mogmeta.to_collection()))
	if mogmeta.equal_meta(dbmeta):
		logger.file_validated(dbmeta)
	else:
		logger.file_corrupted(dbmeta)
		migration_job = {"path": dbmeta["path"], "base": dbmeta["base"]}
		mqclient.send(migration_job)
		msg = procid + " File corrupted. Sent new job to migration queue: " + json.dumps(dbmeta)
		logger.warning(msg)
		stats = "CORRUPTED"
	os.remove(fullpath)
	ch.basic_ack(delivery_tag = method.delivery_tag)
	file_logger.info("%s End validation %s: %s" %(procid, repr(body), stats))

if __name__ == "__main__":
	validation_task = True
	procid = misc.generate_id()
	while validation_task:
		try:
			domain = migconfig.domain
			trackers = migconfig.trackers
			#if len(sys.argv) == 3:
			#	domain = sys.argv[1] if sys.argv[1] else None
			#	listoftracker = sys.argv[2] if sys.argv[2] else None
			#	trackers = listoftracker.split(";")
			trans = MogTransport(domain=domain, trackers=trackers)
			vclient = RMQClient(queue=migconfig.validation_queue)
			mqclient = RMQClient(queue=migconfig.migration_queue)
			logger = MogileLogger()
			file_logger = FileLogger()
			vclient.receive(callback)
		except errors.ConnectionFailure:
			report_error('mongodb')
		except errors.AutoReconnect:
			report_error('mongodb')
		except AMQPConnectionError:
			report_error('rabbitmq')
