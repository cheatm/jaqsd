#! /bin/bash

source /etc/profile
python /jaqsd/jaqsd/finance.py create -s 20170101
python /jaqsd/jaqsd/finance.py check
python /jaqsd/jaqsd/finance.py reach