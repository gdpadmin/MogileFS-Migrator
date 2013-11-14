#!/usr/bin/env python

import json
import sys
import reader
import os

from mogilelogger import FileLogger

def write_to_file(data, name):
        with open(name, 'w') as outputfile:
                json.dump(data, outputfile)

if __name__ == "__main__":
        try:
		logger = FileLogger('/tmp/feeder.log')
        	path = sys.argv[1]
        	suffix = sys.argv[2]
		name = sys.argv[3]

        	result = reader.readAllLevelSuffix(path, suffix)
		write_to_file(result, name)
		fullpath = os.path.join(path, suffix)
		logger.info("Total file in %s: %s " % (fullpath, repr(len(result))))
	except IndexError:
		print "Usage: python feedRecursiveToFile.py [base] [suffix] [output file]"

