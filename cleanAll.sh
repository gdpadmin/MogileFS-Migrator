#!/bin/bash

PWD=$(dirname $0)
RABBIT="${PWD}/rabbitmq.sh clean"
MONGO="${PWD}/mongo.sh clean"
MOGILE="${PWD}/mogtransporttest.py clean"

bash $RABBIT
bash $MONGO
python $MOGILE
