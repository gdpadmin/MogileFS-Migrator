#!/bin/bash

SCRIPT=$(basename $0)
ACTION=$1
PWD=$(pwd)

usage(){
	echo "Usage $SCRIPT [OPTION]"
	echo "Available OPTION"
	echo "start - Start migrator"
	echo "kill - Kill migrator process"
}
killpython(){
	echo "Killing nfstomogile.py process"
	#sudo pkill -9 python
	if [ -f nfstomogile.pid ]; then
		kill -9 `cat nfstomogile.pid`
	else
		echo "Could not find file nfstomogile.pid"
	fi
}
parsebase(){
	TEMPIFS=$IFS
	IFS=/
	RES=""
	FULLPATH=$(readlink -m "$0")
	for i in $FULLPATH; do
		if [ "$i" != "$SCRIPT" ] && [ "$i" != "." ] && [ "$i" != "" ]
		then
			RES="$RES/$i"
		fi	
	done
	IFS=$TEMPIFS
	echo $RES
}

if [ "$ACTION" == "kill" ]; then
	ps ax | grep python
	killpython
	ps ax | grep python
elif [ "$ACTION" == "start" ]; then
	killpython
	BASE=$(parsebase)
	FILE="$BASE/nfstomogmanager.py"
	NOHUP="$PWD/nohup.out"
	if [ -e $FILE ]; then
		# delete nohup file
		rm -f $NOHUP
		# Start migrator
		nohup python $FILE &
		# display nohup output
		echo $! > nfstomogile.pid
		sleep 2
		cat $NOHUP
	else
		echo "$FILE does not exits"
		exit 1
	fi
else
	usage
fi

exit 0 
