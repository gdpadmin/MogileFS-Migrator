#!/usr/bin/env python

from subprocess import call
import shlex

bases = {
	"rabbit" : "sudo service rabbitmq-server ",
	"mongodb" : "sudo service mongodb ",
	"mogilefs" : "sudo service mogilefsd ",
	"mogstored" : "sudo service mogstored "}

actions = {"start": "start", "status": "status", "stop": "stop", "restart": "restart"}

def check_service():
	global bases
	global actions

	for k,v in bases.iteritems():
		cmds = shlex.split(v + actions['status'])
		status = call(cmds)
		print k + ": " + repr(status)

if __name__ == "__main__":
	check_service()