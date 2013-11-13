#!/usr/bin/env python

from pymongo import MongoClient
from pymongo import errors

import json
import logging

"""NFS to MogileFS logging
Interface to communicate with mongodb server
Mongodb user to save logs and files metadata
"""
class MogileLogger:

	def __init__(self):
		self.mongo = MongoClient('localhost', 27017)
		self.db = self.mongo['nfstomogile']
		self.files = self.db['mogfiles']
		self.logger = FileLogger()

	def check(self):
		testing = {"_id": "testingmongo", "msg": "just testing"}
		identry = {"_id": "testingmongo"}
		self.files.insert(testing)
		data = self.files.find_one(identry)
		self.files.remove(identry)
		data = self.files.find_one(identry)
		print "Mongodb ok"

	def info(self, message):
		logging.info(message)

	def error(self, message):
		logging.error(message)

	def warning(self, message):
		logging.warning(message)
		
	def file_get(self, sha1):
		where = {"_id": sha1}
		return self.files.find_one(where)
		
	def file_saved(self, metafile):
		"""Save file metadata to mongodb.
		File metadata is extracted from class FileInfo
		"""
		try:
			if "status" not in metafile:
				metafile["status"] = "saved"
			self.files.insert(metafile)
		except errors.DuplicateKeyError:
			self.logger.error("Duplicate file entry. Keep the old one. " + json.dumps(metafile))
		
		
	def file_validated(self, metafile):
		metafile["status"] = "validated"
		query = {"_id": metafile["_id"]}
		self.files.find_and_modify(query, metafile)
		
	def file_corrupted(self, metafile):
		query = {"_id": metafile["_id"]}
		self.files.remove(query)

class FileLogger:

	class __impl:
		def __init__(self, log_file='/tmp/nfstomogile.log'):
			self.logger = logging.getLogger('NFSToMogile')
			fileHandler = logging.FileHandler(log_file)
			formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
			fileHandler.setFormatter(formatter)
			self.logger.addHandler(fileHandler)
			self.logger.setLevel(logging.INFO)

		def info(self, message):
			self.logger.info(message)

		def error(self, message):
			self.logger.error(message)

		def warning(self, message):
			self.logger.warning(message)
	
	__instance = None
	
	def __init__(self, log_file='/tmp/nfstomogile.log'):
		if FileLogger.__instance is None:
			FileLogger.__instance = FileLogger.__impl(log_file)
		self.__dict__['_FileLogger_instance'] = FileLogger.__instance

	def __getattr__(self, attr):
		return getattr(self.__instance, attr)

	def __setattr__(self, attr, value):
		return setattr(self.__instance, attr, value)

