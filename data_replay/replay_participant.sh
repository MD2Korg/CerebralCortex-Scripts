#!/bin/bash

participant='UUID_OF_PARTICIPANT_DIR'
for f in /cerebralcortex/apiserver/data/$participant/*
do
    if [[ $f != *"*"* ]]; then
    	echo "$f"
    	python3.6 replay_data.py -b dagobah10dot.memphis.edu:9092 -d "$f/"
    fi
done

