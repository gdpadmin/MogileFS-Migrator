import clamdscanner
import sys

if __name__ == "__main__":
	if len(sys.argv) >= 2:
		scanner = clamdscanner.ClamdScanner()
		scanner.init_clamd()
		result = scanner.scan_file(sys.argv[1])
		print "Scan %s with result: %s" %(sys.argv[1], result)
	else:
		print "need file to scan"
