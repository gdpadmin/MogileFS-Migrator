import migconfig
from mogtransport import MogTransport
import sys
import mogilefs

def main():
	domain = migconfig.domain
	trackers = migconfig.trackers
	trans = MogTransport(domain=domain, trackers=trackers)
	try:
		cmd = sys.argv[1]
		if cmd == "clean":
			try:
				trans.delete_all()
			except mogilefs.MogileFSError:
				raise
		elif cmd == "check":
			try:
				trans.key_exist('/')
				print "Trackers available"
			except mogilefs.MogileFSError:
				raise
	except IndexError:
		usage()
	except mogilefs.MogileFSError:
		print "Invalid trackers %s" %(trackers)

def usage():
	print "python mogtransporttest.py [options]"
	print "Available options"
	print "check - Check availability of mogilefs server"
	print "clean - Delete all file on mogilefs"

if __name__ == "__main__":
	main()
