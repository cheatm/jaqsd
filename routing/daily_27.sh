#! /bin/bash

source /etc/profile
python /jaqsd/jaqsd/finance.py write
python /jaqsd/jaqsd/finance.py check
python /jaqsd/jaqsd/finance.py reach
python /jaqsd/jaqsd/finance.py check
