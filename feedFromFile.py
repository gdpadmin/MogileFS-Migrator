#!/usr/bin/env python

import json
import sys
import json
import migconfig
import re

from mogilelogger import FileLogger
from writer import RMQClient
from pika.exceptions import AMQPConnectionError


def read_json_from_file(filename):
	json_data = open(filename)
	data = json.load(json_data)
	json_data.close()
	return data

if __name__ == "__main__":
	try:
		logger = FileLogger('/tmp/feeder.log')
        	file_name = sys.argv[1]
        	result = read_json_from_file(file_name)
        	success = 0
        	total = len(result)
                migrationmq = RMQClient(queue=migconfig.migration_queue)
                for job in result:
			job["path"] = re.sub('^/', '', job["path"])
			json_job = json.dumps(job)
                        migrationmq.send(json_job)
                        success = success + 1
                        logger.info("Successfully sent %s" % (json_job))
		status = "OK" if success == total else "NOT OK"
		logger.info("Status: %s, Stats %s: success %s, total %s" % (status, file_name, success, total))
        except AMQPConnectionError:
                logger.info("RabbitMQ Connection error")
	except IndexError:
		print "Usage: python feedFromFile.py [source file]"
