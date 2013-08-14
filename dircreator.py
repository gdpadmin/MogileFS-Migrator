#!/usr/bin/env python

import os
import sys
import logging
import shutil

strMonth = lambda x: str(x) if x > 9 else "0" + str(x)

"""
This program is intended to create directory structure and files at kaskus at 2013
This is needed to simulate migration process from currently used file system (NFS) to mogilefs.
"""

class DirCreator(object):
	
	def __init__(self, logger, path, count):
		self.data = [
			['2012', '2013', 'apps', 'banner', 'banner-wap', 'editor', 'group', 'smilies'],
			{'2012' : range(5, 13), '2013': range(1, 7), 'banner-wap': ['2013'], 'smilies': ['sumbangan']},
			{'banner-wap|2013': range(5,7),
			'2012|05': range(4, 31),
			'2012|06': range(1, 31),
			'2012|08': range(1, 31),
			'2012|07': range(1, 32),
			'2012|10': range(1, 32),
			'2012|12': range(1, 32),
			'2012|09': range(1, 31),
			'2012|11': range(1, 31),
			'2013|01': range(1,32),
			'2013|02': range(1,29),
			'2013|03': range(1,32),
			'2013|05': range(1,32),
			'2013|04': range(1,31),
			'2013|06': range(1,13)},
			{'banner-wap|2013|05': [8, 10, 17, 23, 24, 27, 28, 29, 31], 'banner-wap|2013|06': [3, 7, 10]}
		]
		self.interval = {'31': [1,3,5,7,8,10,12], '30': [4,6,9,11], '28': 2}
		self.logger = logger
		self.file = path
		self.count = count

	def createFolder(self, dir):
		"""Create a folder
		
		Keyword arguments:
		dir -- directory that need to be created
		"""
		if isinstance(dir, int):
			dir = strMonth(dir)
			
		if not os.path.exists(dir):
			os.mkdir(dir)
			self.logger.debug('Create directory: %s', dir)
		else:
			self.logger.debug('Directory already exits: %s', dir)
		os.chdir(dir)
		self.replicateFile()
		os.chdir("..")

	def createFolders(self, dirs):
		"""Create several directories at once
		
		Keyword arguments:
		dirs -- list of directory that need to be created
		"""
		for dir in dirs:
			self.createFolder(dir)
	
	def createLevelRoot(self):
		"""Create root directory from self.data value
		
		"""
		currentDir = os.getcwd()
		self.logger.info('Create level one directories under %s', currentDir)
		folders = self.data[0]
		for val in folders:
			self.createFolder(val)
	
	def createLevel(self, level):
		"""Create directories at given level
		
		Keyword arguments:
		level -- Place where directories created
		"""
		folders = self.data[level]
		base = os.getcwd()
		self.logger.info('Create level %d directories under %s', level, base)
		for k,dirs in folders.iteritems():
			path = k.replace('|', os.sep)
			self.logger.debug('Path: %s', path)
			curDir = os.path.join(base, path)
			os.chdir(curDir)
			self.logger.debug('Working directory: %s', os.getcwd())
			self.createFolders(dirs)
			os.chdir(base)
	
	def setWorkingDirectory(self, dir):
		"""Setup working directory based on parameter from user
		
		Keyword arguments:
		dir -- intended working directory
		"""
		workingDir = dir
		if os.path.isdir(workingDir):
			shutil.rmtree(workingDir)
		
		os.makedirs(workingDir)
		os.chdir(workingDir)
		workingDir = os.getcwd()
		self.logger.info("Current working directory {0}".format(workingDir))
	
	def replicateFile(self):
		"""Replicate given file inside new created directory
		Number of replication is specify at runtime parameter.
		Default replication count is 20.
		
		"""
		dir = os.getcwd()
		basename = os.path.basename(self.file)
		for i in range(0, self.count):
			filename = "rep-" + str(i) + basename
			filename = os.path.join(dir, filename)
			self.logger.debug('Copying %s to %s', self.file, filename)
			shutil.copy2(self.file, filename)

"""
Entry point to create directory structure
Usage ./dirCreator.py [working directory] [file to replicate] [number of replication]
"""
def main():
	logger = logging.getLogger('dirCreator')
	ch = logging.StreamHandler()
	ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
	logger.addHandler(ch)
	logger.setLevel(logging.DEBUG)
	logger.info('Start dirCreator program')
	
	if (len(sys.argv) > 2):
		dir = sys.argv[1]
		file = sys.argv[2]
		number = int(sys.argv[3]) if sys.argv[3] else 20
	else:
		logger.error('Usage: python dirCreator.py [directory] [file] [number of files]')
		sys.exit()
	
	abspath = os.path.abspath(file)
	if not os.path.isfile(file):
		logger.error('File not exists')
		sys.exit()
	
	mock = DirCreator(logger=logger, path=abspath, count=number)
	mock.setWorkingDirectory(dir)
	mock.createLevelRoot()
	for level in range(1, 4):
		mock.createLevel(level)

if __name__ == "__main__":
	main()