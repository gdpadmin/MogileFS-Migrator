#!/usr/bin/env python

import reader
import sys
import json
import migconfig

from mogilelogger import FileLogger
from writer import RMQClient
from pika.exceptions import AMQPConnectionError

if __name__ == "__main__":
	logger = FileLogger()
	path = sys.argv[1]
	logger.info("Start reading file")
	result = reader.readAllLevel(path)
	logger.info("Finish reading, start sending to MQ")
	success = 0
	total = len(result)
	
	try:	
		migrationmq = RMQClient(queue=migconfig.migration_queue)
		for job in result:
			json_job = json.dumps(job)
			migrationmq.send(json_job)
			success = success + 1
			logger.info("Successfully sent %s" % (json_job))
	except AMQPConnectionError:
		logger.info("RabbitMQ Connection error")
	logger.info("Statistic: success %s, total %s" % (success, total))