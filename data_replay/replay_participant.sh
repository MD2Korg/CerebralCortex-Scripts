#!/bin/bash

for p in /cerebralcortex/apiserver/data/[1-9a-z]*-[a-z0-9]*-[a-z0-9]*
do
    participant=$(basename $p)

    for f in /cerebralcortex/apiserver/data/$participant/201712*
    do

	if [[ $f != *"*"* ]]; then
	    echo "$f"
	    python3.6 replay_data.py -b dagobah10dot.memphis.edu:9092 -d "$f/"
	fi
	sleep 1s
    done
    #sleep 1m
done
