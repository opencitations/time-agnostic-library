#!/bin/sh

# set the recommended number of rdf loaders (total number of cores / 2.5)
TOTAL_CORES=$(cat /proc/cpuinfo | grep processor | wc -l)
NUMBER_OF_LOADERS=$(awk "BEGIN {printf \"%d\", $TOTAL_CORES/2.5}")

#!/bin/bash
ADDRESS=$1
PORT=1111
FOLDER=$2
GRAPHURI=$3
VIRTUOSO_BIN=/usr/local/virtuoso-opensource/bin

start_load=$(($(date +%s%N)/1000000))
$VIRTUOSO_BIN/isql-v $ADDRESS:$PORT exec="delete from load_list;" > /dev/null 2>&1
$VIRTUOSO_BIN/isql-v $ADDRESS:$PORT exec="DB.DBA.RDF_OBJ_FT_RULE_DEL (null, null, 'ALL');" > /dev/null 2>&1
$VIRTUOSO_BIN/isql-v $ADDRESS:$PORT exec="ld_dir('"$FOLDER"', '*', '"$GRAPHURI"');" > /dev/null 2>&1
for i in `seq 1 $NUMBER_OF_LOADERS`;
do
    $VIRTUOSO_BIN/isql-v $ADDRESS:$PORT exec="rdf_loader_run()" & > /dev/null 2>&1
done
wait

$VIRTUOSO_BIN/isql-v $ADDRESS:$PORT exec="checkpoint;" > /dev/null 2>&1
end_load=$(($(date +%s%N)/1000000))
loadingtime=$(($end_load - $start_load))

sync

# logging
echo "All data loaded to graph <"$GRAPHURI">, using "$NUMBER_OF_LOADERS" rdf loaders. Time : "$loadingtime" ms"

