#!/usr/bin/env python

from mogilelogger import FileLogger

import socket
import time

sleep_time = 10
logger = FileLogger()

"""Collections of misc functions/classes"""

def to_numeric(x, default=0):
	try:
		numeric = int(x)
		return numeric
	except ValueError:
		return default

def report_error(message, sleep=True):
	logger.error("Component error: " + message)
	if sleep: time.sleep(sleep_time)
	logger.info("Wake up: " + message)