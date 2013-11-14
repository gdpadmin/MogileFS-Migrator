#!/usr/bin/env python

import json
import sys
import reader

from mogilelogger import FileLogger

def write_to_file(data, name):
        with open(name, 'w') as outputfile:
                json.dump(data, outputfile)

if __name__ == "__main__":
        logger = FileLogger('/tmp/feeder.log')
        path = sys.argv[1]
        logger.info("Start reading file")
        suffix = sys.argv[2]
        result = reader.readLevel(path, suffix)
	write_to_file(result['files'], 'files.json')
	write_to_file(result['directories'], 'directory.json')

