#!/bin/bash

CURR_VERSION_FILE=$1
CW_TBD=$2
DESTINATION_PATH=$3

filename=$(basename "$CURR_VERSION_FILE")
filename="${filename%added.*}"
grep -F -f $CW_TBD $CURR_VERSION_FILE >> $DESTINATION_PATH/$filename"deleted.nt"
