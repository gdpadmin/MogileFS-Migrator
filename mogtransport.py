from mogilefs import Client
from mogilelogger import FileLogger
import os

class MogTransport:
	
	def __init__(self, domain=None, trackers=None):
		self.logger = FileLogger()
		if domain is None:
			domain = 'kaskus'
		if trackers is None:
			trackers = ['localhost:7001']
		self.client = Client(domain, trackers)

	def send_file(self, key, source, clas=None, force=False):
		res = True
		if self.key_exist(key):
			if force:
				self.delete(key)
				res = self.client.send_file(key=key, source=source, clas=clas)
			else:
				res = None
		else:
			res = self.client.send_file(key=key, source=source, clas=clas)
		return res

	def key_exist(self, key):
		keys = self.client.list_keys(key)
		return False if not keys[0] else True

	def delete(self, key):
		return self.client.delete(key)

	def delete_keys(self, keys):
		for key in keys:
			print "Deleting key %s" % (key,)
			self.delete(key)

	def delete_all(self):
		self.loop_delete('/')
		for pref in range(97, 123):
			self.loop_delete(chr(pref))
		for number in range(1, 10):
			#keys = self.client.list_keys(str(number))
			#if keys[0]:
			#	self.delete_keys(keys[1])
			self.loop_delete(str(number))
	
	def loop_delete(self, key):
		after = None
		cont = True
		while cont:
			keys = self.client.list_keys(prefix=key, after=after)
			if keys[0]:
				self.delete_keys(keys[1])
				after = keys[0]
			else:
				cont = False

	def download_file(self, key, name=None):
		fname = name if name is not None else os.path.basename(key)
		fd = open(fname, 'w')
		res = self.client.get_file_data(key, fp=fd)
		fd.close()
		return res

	def list_keys(self, key):
		#list_keys(self, prefix, after=None, limit=None):
		keys = self.client.list_keys(prefix=key)
		return keys
