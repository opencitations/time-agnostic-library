#!/bin/bash

VIRTUOSO_BIN=/usr/local/virtuoso-opensource/bin
GRAPH_NAME=http://graph.version.
DATASETS_PATH=/versioning/data
DATASETS_PATH_FINAL=/versioning/data/final
NUMBER_OF_VERSIONS=$1
total_cores=$(cat /proc/cpuinfo | grep processor | wc -l)
rdf_loaders=$(awk "BEGIN {printf \"%d\", $total_cores/2.5}")

echo "total cores: $total_cores"
prll_rdf_loader_run() {
   $VIRTUOSO_BIN/isql-v 1111 dba dba exec="set isolation='uncommitted';" > /dev/null
   for ((j=0; j<$1; j++)); do  
      $VIRTUOSO_BIN/isql-v 1111 dba dba exec="rdf_loader_run();" > /dev/null &
   done
   wait
   $VIRTUOSO_BIN/isql-v 1111 dba dba exec="checkpoint;" > /dev/null
   
   # if there are files that failed to be loaded reload them until they succeed
   errors=$($VIRTUOSO_BIN/isql-v 1111 dba dba exec="select count(*) from load_list where ll_error is not null;" | sed -n 9p)
   files=$($VIRTUOSO_BIN/isql-v 1111 dba dba exec="select ll_file from load_list where ll_error is not null;" | sed '1,8d' | head -n $errors)

   while [ "$errors" -gt "0" ]; do
      echo "The following "$errors" file(s) failed to be loaded. "
  	  echo $files
  	  echo "Retrying..."
	  $VIRTUOSO_BIN/isql-v 1111 dba dba exec="update load_list set ll_state = 0, ll_error = null where ll_error is not null;" > /dev/null
      for ((j=0; j<$1; j++)); do  
         $VIRTUOSO_BIN/isql-v 1111 dba dba exec="rdf_loader_run();" > /dev/null &
      done
      wait
      $VIRTUOSO_BIN/isql-v 1111 dba dba exec="checkpoint;" > /dev/null
	  errors=$($VIRTUOSO_BIN/isql-v 1111 dba dba exec="select count(*) from load_list where ll_error is not null;" | sed -n 9p)
   done
   
   $VIRTUOSO_BIN/isql-v 1111 dba dba exec="checkpoint;" > /dev/null
   $VIRTUOSO_BIN/isql-v 1111 dba dba exec="set isolation='committed';" > /dev/null
   $VIRTUOSO_BIN/isql-v 1111 dba dba exec="delete from load_list;" > /dev/null
   echo "All data files loaded successfully"
}

# prepare bulk loadmkdir $DATASETS_PATH_FINAL
for ((i=0; i<$NUMBER_OF_VERSIONS; i++)); do
   $VIRTUOSO_BIN/isql-v 1111 dba dba exec="ld_dir('$DATASETS_PATH_FINAL/v$i', '*', '$GRAPH_NAME$i');" > /dev/null
done

# bulk load
echo "Loading data files into virtuoso using $rdf_loaders rdf loaders..."
start_load=$(($(date +%s%N)/1000000))
prll_rdf_loader_run $rdf_loaders
end_load=$(($(date +%s%N)/1000000))

for ((j=0; j<$NUMBER_OF_VERSIONS; j++)); do
   result=$($VIRTUOSO_BIN/isql-v 1111 dba dba exec="sparql select count(*) from <$GRAPH_NAME$j> where { ?s ?p ?o };" | sed -n 9p) > /dev/null
   echo $(echo $result | sed ':a;s/\B[0-9]\{3\}\>/,&/;ta') "triples loaded to graph <"$GRAPH_NAME$j">"
done
end_size=$(($(date +%s%N)/1000000))

loadingtime=$(($end_load - $start_load))
sizetime=$(($end_size - $end_load))
overalltime=$(($end_size - $start_load))

echo "Loading of all generated data to Virtuoso triple store completed successfully. Time: $overalltime ms (loading: $loadingtime, size: $sizetime)"

