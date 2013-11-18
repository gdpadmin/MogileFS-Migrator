#!/usr/bin/env python

import json
import sys
import reader
import os

from mogilelogger import FileLogger

def write_to_file(data, name):
        with open(name, 'w') as outputfile:
                for path in data:
			outputfile.write(path["path"] + "\n")

if __name__ == "__main__":
        try:
		logger = FileLogger('/tmp/feeder.log')
        	path = sys.argv[1]
        	suffix = sys.argv[2]
		name = sys.argv[3]

        	result = reader.readLevel(path, suffix)
		write_to_file(result['files'], name)
		filename_dir = name + "-directories"
		write_to_file(result['directories'], filename_dir)	
		fullpath = os.path.join(path, suffix)
		logger.info("Total file in %s: %s " % (fullpath, repr(len(result))))
	except IndexError:
		print "Usage: python feedPlainToFile.py [base] [suffix] [output file]"

