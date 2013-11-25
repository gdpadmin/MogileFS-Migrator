import socket
import sys

class MigratorClientSocket:
	
	def __init__(self, sock=None):
		if sock is None:
			self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		else:
			self.sock = sock

	def connect(self, host='localhost', port=12012):
		self.sock.connect((host, port))

	def send(self, msg):
		self.sock.send(msg)
		reply = self.sock.recv(16384)
		return reply

	def send_reset_migrator(self):
		reply = self.send('!reset_migrator')
		print reply

	def send_reset_validator(self):
		reply = self.send('!reset_validator')
		print reply

	def send_reset(self):
		self.send_reset_migrator()
		self.send_reset_validator()

	def send_stats(self):
		reply = self.send('!stats')
		print reply

	def close(self):
		self.sock.close()

if __name__ == "__main__":
	cmd = sys.argv[1]
	client = MigratorClientSocket()
	client.connect()
	print "Stats before reset"
	client.send_stats()
	if cmd == "migrator":
		client.send_reset_migrator()
	elif cmd == "validator":
		client.send_reset_validator()
	elif cmd == "both":
		client.send_reset()
	else:
		print "No Op"
	print "Stats after reset"
	client.send_stats()
	client.close()
