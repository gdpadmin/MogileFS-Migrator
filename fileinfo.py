import os
import hashlib
import json

"""Represent file data structure
This datastructure will be used by FileReader
"""
class FileInfo:
	
	def __init__(self, base=None, path=None):
		self.base = base
		self.file = path
	
	def set_absolute_file(self, base, path):
		self.base = base
		self.file = path
	
	def get_filename(self):
		if self.is_valid():
			return os.path.basename(self.file)
		else:
			return None
	
	def get_absolute_path(self):
		return os.path.join(self.base, self.file)

	def calculate_sha1(self, block_size=160):
		result = None
		if self.is_valid():
			sha1 = hashlib.sha1()
			fd = open(self.get_absolute_path())
			sha1.update(fd.read())
			result = sha1.hexdigest()
		return result
	
	def is_valid(self):
		abspath = self.get_absolute_path()
		return self.file is not None and os.path.isfile(abspath)

	def get_size(self):
		size = 0
		if self.is_valid():
			size = os.path.getsize(self.get_absolute_path())
		return size

	def equal(self, fileinfo):
		result = self.is_valid() and fileinfo.is_valid()
		if result:
			result = self.get_size() == fileinfo.get_size()

		if result:
			result = fileinfo.calculate_sha1() == self.calculate_sha1()
		return result

	def equal_meta(self, fileinfo):
		return fileinfo["size"] == self.get_size() and \
			fileinfo["_id"] == self.calculate_sha1()
	
	def to_collection(self):
		size = self.get_size()
		sha1 = self.calculate_sha1()
		path = self.file
		base = self.base
		result = {"size": size, "_id": path, "path": path, "base": base}
		return result

	def to_string(self):
		coll = self.to_collection()
		return json.dumps(coll)
