#!/bin/bash

sudo rabbitmqctl list_queues name messages_ready messages_unacknowledged

exit 0
