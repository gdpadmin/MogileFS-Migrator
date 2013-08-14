#!/bin/bash

BASE=~
# Kill python process
sudo pkill -9 python
# delete nohup file
rm $BASE/nohup.out
# Start migrator
nohup python $BASE/nfstomogile/nfstomogmanager.py &
# display nohup outpu
sleep 2
cat $BASE/nohup.out

exit 0 
