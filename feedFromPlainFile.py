#!/usr/bin/env python

import json
import sys
import migconfig
import re

from mogilelogger import FileLogger
from writer import RMQClient
from pika.exceptions import AMQPConnectionError

migrationmq = None
success = 0
logger = FileLogger('/tmp/feeder.log')

def read_from_file_and_send(filename, base):
	global migrationmq
	global success
	
	with open(filename) as infile:
		for line in infile:
			job = {"base": base, "path": line.strip()}
			job["path"] = re.sub('^/', '', job["path"])
			json_job = json.dumps(job)
			migrationmq.send(json_job)
			success = success + 1
			logger.info("Successfully sent %s" % (json_job))
		logger.info("Stats %s: success %s" % (file_name, success))
		print "Stats %s: success %s" % (file_name, success)

if __name__ == "__main__":
	try:
        	file_name = sys.argv[1]
        	base = sys.argv[2]
                migrationmq = RMQClient(queue=migconfig.migration_queue)
        	read_from_file_and_send(file_name, base)
	except AMQPConnectionError:
                logger.info("RabbitMQ Connection error")
	except IndexError:
		print "Usage: python feedFromPlainFile.py [source file] [base]"
