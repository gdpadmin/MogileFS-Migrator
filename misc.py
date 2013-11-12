#!/usr/bin/env python

from mogilelogger import FileLogger

import time
import string
import random

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

def generate_id(size=6, chars=string.ascii_uppercase + string.digits):
	return ''.join(random.choice(chars) for x in range(size))
