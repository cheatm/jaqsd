#! /bin/bash

if [ -n "$1" ]
then
    INIT_LB_DAILY=$1
fi

if [ -n "$2" ]
then
    INIT_INDICATOR=$2
fi

if [ -n "$3" ]
then
    SYNC_TABLE=$3
fi

python jaqsd/init.py lb_daily $INIT_LB_DAILY
python jaqsd/init.py indicator $INIT_INDICATOR
if [ -n "$SYNC_TABLE" ]
then
    python jaqsd/table.py create $SYNC_TABLE
fi

/usr/sbin/cron -f

