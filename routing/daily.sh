#! /bin/bash

source /etc/profile

jaqsd instrument write

python /jaqsd/jaqsd/collector/daily.py

jaqsd daily_index write

jaqsd finance write
jaqsd finance check
jaqsd finance reach
jaqsd finance check

jaqsd indicator check
jaqsd indicator write
jaqsd indicator check

jaqsd future contract
jaqsd future write
jaqsd future check

jaqsd table sync