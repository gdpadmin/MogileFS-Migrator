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

def report_error(message):
	global vclient
	global mqclient

	vclient.close()
	mqclient.close()
	errormsg = "Validator task report error: " + message
	misc.report_error(errormsg)

def callback(ch, method, properties, body):
	print "Start validation: %s" % (body, )
	dbmeta = logger.file_get(body)
	print "Database meta data: %s" % (dbmeta)
	base = "/tmp"
	name = os.path.basename(dbmeta["path"])
	fullpath = os.path.join(base, name)
	trans.download_file(key=dbmeta["path"], name=fullpath)
	mogmeta = FileInfo("/tmp", name)
	print "MogileFS meta data: %s" % (mogmeta.to_collection())
	if mogmeta.equal_meta(dbmeta):
		logger.file_validated(dbmeta)
	else:
		logger.file_corrupted(dbmeta)
		migration_job = {"path": dbmeta["path"], "base": dbmeta["base"]}
		mqclient.send(migration_job)
		msg = "File corrupted. Sent new job to migration queue: " + json.dumps(dbmeta)
		logger.warning(msg)
	os.remove(fullpath)
	ch.basic_ack(delivery_tag = method.delivery_tag)
	print "End validation: %s" % (body,)

if __name__ == "__main__":
	validation_task = True
	while validation_task:
		try:
			domain = None
			trackers = None
			if len(sys.argv) == 3:
				domain = sys.argv[1] if sys.argv[1] else None
				listoftracker = sys.argv[2] if sys.argv[2] else None
				trackers = listoftracker.split(";")
			trans = MogTransport(domain=domain, trackers=trackers)
			vclient = RMQClient(queue="validation_job_queue")
			mqclient = RMQClient(queue="migration_job_queue")
			logger = MogileLogger()
			vclient.receive(callback)
		except errors.ConnectionFailure:
			report_error('mongodb')
		except errors.AutoReconnect:
			report_error('mongodb')
		except AMQPConnectionError:
			report_error('rabbitmq')