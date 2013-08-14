import os
import pika

class RMQClient:

	def __init__(self, host='127.0.0.1', queue=''):
		self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
		self.channel = self.connection.channel()
		self.queue = queue
		self.channel.queue_declare(queue=self.queue, durable=True)

	def send(self, body):
		self.channel.basic_publish(exchange='',
			routing_key=self.queue,
			body=body,
			properties=pika.BasicProperties(
				delivery_mode = 2
			))

	def receive(self, callback):
		self.channel.basic_qos(prefetch_count=1)
		self.channel.basic_consume(callback,
			queue=self.queue)
		self.channel.start_consuming()

	def get(self):
		method_frame, header_frame, body = self.channel.basic_get(queue=self.queue)
		if method_frame.NAME == 'Basic.GetEmpty':
			return None
		else:
			self.channel.basic_ack(delivery_tag=method_frame.delivery_tag)
			return body

	def get_message_count(self):
		status = self.channel.queue_declare(queue=self.queue, durable=True)
		return status.method.message_count

	def close(self):
		#self.connection.close()
		pass
