#!/bin/bash

for index in {1..7}
do
  day=`cat /output1.txt | head -n $index | tail -n 1 | cut -f 1`
  sum_fares=`cat /output1.txt | head -n $index | tail -n 1 | cut -f 2`
  count=`cat /output2.txt | head -n $index | tail -n 1 | cut -f 2`
  
  avg=`echo "scale=2; $sum_fares/$count" | bc`
  echo "Day: $day - average fare amount: $avg"
done
