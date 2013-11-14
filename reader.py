import os
import array

def readAllDir(path):
	"""Read all file name under given path recursively
	Keywords arguments:
	path -- location where file will be read
	return list of files
	"""
	result = []
	if os.path.isdir(path):
		for path, dirs, files in os.walk(path):
			for f in files:
				result.append(os.path.join(path, f))
	return result


def readAllLevelSuffix(base, suffix):
	result = []
        start = base
	start_with_slash = start + '' if start[-1] == '/' else '/'
	base_with_suffix = os.path.join(base, suffix)
        if os.path.isdir(base_with_suffix):
                for path, dirs, files in os.walk(base_with_suffix):
                        for f in files:
                                dinamic = path.replace(start, "")
                                metadata = {"base": start, "path": os.path.join(dinamic, f)}
                                result.append(metadata)
        return result

def readAllLevel(path):
	result = []
	start = path
	if os.path.isdir(path):
		for path, dirs, files in os.walk(path):
			for f in files:
				dinamic = path.replace(start, "")
				metadata = {"base": start, "path": os.path.join(dinamic, f)}
				result.append(metadata)
	return result

def readDir(path):
	"""Read all file name under given directory
	
	Keyword arguments:
	path -- location where file will be read
	return list of files
	"""
	result = []
	if os.path.isdir(path):
		listing = os.listdir(path)
		for file in listing:
			abspath = os.path.join(path, file)
			if os.path.isfile(abspath):
				result.append(abspath)
	return result

def readLevel(base, path):
	result = {'directories': [], 'files': []}
	fullpath = os.path.join(base, path)
	if os.path.isdir(fullpath):
		print "fullpath %s" % (fullpath)
		listing = os.listdir(fullpath)
		for name in listing:
			abspath = os.path.join(fullpath, name)
			metadata = {"base": base, "path": os.path.join(path, name)}
			if os.path.isfile(abspath):
				result['files'].append(metadata)
			else:
				result['directories'].append(metadata)
	return result

