#!/bin/bash

SCRIPT=$(basename $0)
ACTION=$1

usage(){
	echo "Usage $SCRIPT [OPTION]"
	echo "Available OPTION"
	echo "clean - Delete nfstomogile database"
	echo "list - List database"
}

if [ "$ACTION" == "clean" ]; then
	mongo nfstomogile --eval "db.dropDatabase()"
elif [ "$ACTION" == "list" ]; then
	echo -e "use nfstomogile\nshow collections\ndb.mogfiles.find()" | mongo
else
	usage
fi

exit 0
