#! /bin/bash

source /etc/profile
python /jaqsd/jaqsd/indicator.py check
python /jaqsd/jaqsd/indicator.py write
python /jaqsd/jaqsd/indicator.py check

