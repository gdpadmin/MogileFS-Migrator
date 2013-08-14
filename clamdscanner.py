#!/usr/bin/python

import pyclamd

class ClamdScanner:
	
	def scan_file(self, fname):
		result = False
		if self.result:
        		try:
                		pyclamd.scan_file(fname)
				result = True
        		except pyclamd.ScanError:
                		raise
		return result

	def init_clamd(self, us=None, ns=None):
		self.result = False
		try:
			if us is not None:
				pyclamd.init_unix_socket(us)
			else:
				pyclamd.init_unix_socket()
			self.result = True
		except pyclamd.ScanError:
			try:
				if ns is not None:
					pyclamd.init_network_socket(ns[0], ns[1])
				else:
					pyclamd.init_network_socket()
				self.result = True
			except pyclamd.ScanError:
				raise

