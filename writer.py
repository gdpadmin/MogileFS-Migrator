import os
import pika

class RMQClient:

	def __init__(self, host='127.0.0.1', queue=''):
		self.parameters = pika.ConnectionParameters(host)
		self.connection = pika.BlockingConnection(self.parameters)
		self.channel = self.connection.channel()
		self.queue = queue
		self.channel.queue_declare(queue=self.queue, durable=True, auto_delete=False)

	def add_timeout(self, callback, timeout=60):
		self.connection.add_timeout(timeout, callback)
	
	def reconnect(self):
		self.channel.basic_consume(self.callback, queue=self.queue)
		self.channel.start_consuming()

	def default_timeout_callback(self):
		self.add_timeout(self.reconnect)
	
	def send(self, body):
		self.channel.basic_publish(exchange='',
			routing_key=self.queue,
			body=body,
			properties=pika.BasicProperties(
				delivery_mode = 2
			))

	def receive(self, callback):
		self.callback = callback
		self.default_timeout_callback()
		self.channel.basic_qos(prefetch_count=1)
		self.channel.basic_consume(callback,
			queue=self.queue)
		self.channel.start_consuming()

	def get(self):
		try:
			method_frame, header_frame, body = self.channel.basic_get(queue=self.queue)
			if method_frame.NAME == 'Basic.GetEmpty':
				return None
			else:
				self.channel.basic_ack(delivery_tag=method_frame.delivery_tag)
				return body
		except AttributeError:
			pass
		return False

	def get_message_count(self):
		status = self.channel.queue_declare(queue=self.queue, durable=True)
		return status.method.message_count

	def close(self):
		#self.connection.close()
		pass
