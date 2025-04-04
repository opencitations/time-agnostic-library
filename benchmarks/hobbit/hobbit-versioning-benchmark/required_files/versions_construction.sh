#!/bin/bash

DATASETS_PATH=/versioning/data
DATASETS_PATH_FINAL=/versioning/data/final
ONTOLOGIES_PATH=/versioning/ontologies
NUMBER_OF_VERSIONS=$1

# prepare cw data files for loading
# sort files
start_sort=$(($(date +%s%N)/1000000))
for f in $(find $DATASETS_PATH -name 'generatedCreativeWorks-*.nt'); do 
   sort "$f" -o "$f"
done
end_sort=$(($(date +%s%N)/1000000))
sorttime=$(($end_sort - $start_sort))
echo "Generated creative works sorted successfully in $sorttime ms."

# copy and compute the addsets 
start_prepare=$(($(date +%s%N)/1000000))
mkdir $DATASETS_PATH_FINAL
for ((i=0; i<$NUMBER_OF_VERSIONS; i++)); do
   start_v_construction=$(($(date +%s%N)/1000000))
   echo "Constructing v$i..."
   if [ "$i" = "0" ]; then
      mkdir $DATASETS_PATH_FINAL/v$i
      cp $DATASETS_PATH/v$i/generatedCreativeWorks*.nt $DATASETS_PATH_FINAL/v$i
      cp $DATASETS_PATH/v$i/dbpedia_final/*.nt $DATASETS_PATH_FINAL/v$i
      cp $ONTOLOGIES_PATH/*.nt $DATASETS_PATH_FINAL/v$i
   else
      mkdir $DATASETS_PATH_FINAL/v$i
      cp $ONTOLOGIES_PATH/* $DATASETS_PATH_FINAL/v$i
      prev=$((i-1))

      # dbpedia
      # if current version contains dbpedia copy the dbpedia version, else copy the previous version
      if ls $DATASETS_PATH/c$i/dbpedia_final/dbpedia_*_1000_entities.nt 1> /dev/null 2>&1; then
        # copy the current version
        cp $DATASETS_PATH/c$i/dbpedia_final/dbpedia_*_1000_entities.nt $DATASETS_PATH_FINAL/v$i
      else
	 cp $DATASETS_PATH_FINAL/v$prev/dbpedia_*.nt $DATASETS_PATH_FINAL/v$i
      fi
      
      # creative works
      if ls $DATASETS_PATH/c$i/generatedCreativeWorks-*.deleted.nt 1> /dev/null 2>&1; then
         # compute the old creative works that still exist
         for f in $DATASETS_PATH_FINAL/v$prev/generatedCreativeWorks*.added.nt; do
            comm_command="comm -23 $f "
            for ff in $DATASETS_PATH/c$i/generatedCreativeWorks*.deleted.nt; do
               comm_command+="$ff | comm -23 - "
            done
            filename=$(basename "$f")
            comm_command=${comm_command::-14}
            eval $comm_command > $DATASETS_PATH_FINAL/v$i/$filename &
         done
         wait
      else
         # copy the previous added
         cp $DATASETS_PATH_FINAL/v$prev/generatedCreativeWorks*.added.nt $DATASETS_PATH_FINAL/v$i
      fi
      # copy the current added
      cp $DATASETS_PATH/c$i/generatedCreativeWorks*.added.nt $DATASETS_PATH_FINAL/v$i
   fi
   end_v_construction=$(($(date +%s%N)/1000000))
   v_construction=$(($end_v_construction - $start_v_construction))
   echo "v$i constructed successfully in $v_construction ms"
done

