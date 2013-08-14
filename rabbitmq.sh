#!/bin/bash

BASESCRIPT=$(basename $0)
ACTION=$1

usage(){
	echo "Usage $BASESCRIPT [action]"
	echo "Available actions"
	echo "list - List all channels"
	echo "clean - Delete all message on queue"
}

if [ "$ACTION" == "list" ]; then
	sudo rabbitmqctl list_queues name messages_ready messages_unacknowledged
elif [ "$ACTION" == "clean" ]; then
	sudo rabbitmqctl stop_app
	sudo rabbitmqctl reset
	sudo rabbitmqctl start_app
else
	usage
fi

exit 0
