#!/bin/sh

VIRTUOSO_BIN=/usr/local/virtuoso-opensource/bin

$VIRTUOSO_BIN/isql-v 1111 dba dba exec="checkpoint;" > /dev/null
