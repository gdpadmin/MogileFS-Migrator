from subprocess import Popen, PIPE

def get_defunct_process():
	ps = Popen(["ps", "ax"], stdout=PIPE)
        grep_python = Popen(["grep", "python"], stdin=ps.stdout, stdout=PIPE)
        grep_defunct = Popen(["grep", "defunct"], stdin=grep_python.stdout, stdout=PIPE)
        wc = Popen(["wc", "-l"], stdin=grep_defunct.stdout, stdout=PIPE)
        result = wc.communicate()[0]
	return result.strip()

if __name__ == "__main__":
	result = get_defunct_process()
	if result.strip() == "0":
		print "Everything is fine"
	else:
		print "Broken process: " + repr(result)
		reset = Popen(["python", "/home/roy/MogileFS-Migrator/resetViaSocket.py", "both"])
		output,error = reset.communicate()
		print output
