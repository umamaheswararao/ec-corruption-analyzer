#!/bin/bash
#provide all data dir paths by comma separated
DATADIRS=/dataroot/ycloud,/dataroot/ycloud1

TMP_ALL_ZERO_BLKS_RESULT=/tmp/all_zero_blks_result.txt
TMP_ALL_BLK_PATHS=/tmp/all_blk_paths_result.txt
HDFS_OUTPUT_DIR=/scanning/


rm -rf $TMP_ALL_ZERO_BLKS_RESULT
rm -rf $TMP_ALL_BLK_PATHS

#Scans for the block timestamps and  all zeros blocks
findBlkTimeStampAndAllZeroBlocksInDir(){
  echo "Scanning dir:" $1
  LOCAL_OUT_FILE=$2
  blocks=$(find $1 -type f -iname "*blk_-*" | awk '!/.meta/{print }' | awk '/BP-/{print}')
  #echo $blocks
  for word in $blocks
  do
      #echo The word is $word
      BLK_PATH_WITH_TIME_STAMP=$(stat -c '%.9Y %n' $word)
      echo $BLK_PATH_WITH_TIME_STAMP >>$TMP_ALL_BLK_PATHS
      
      #Find if it's a allzero block
      compare=$(cmp  $word /dev/zero)
      if [[ $compare = "" ]]; then echo $word | uniq >>$LOCAL_OUT_FILE; fi
  done
}

# Loop over to all data directories and call findBlkTimeStampAndAllZeroBlocksInDir function for finding timestamp and all zero blocks
for DATADIR in $(echo $DATADIRS | sed "s/,/ /g")
do
    findBlkTimeStampAndAllZeroBlocksInDir $DATADIR $TMP_ALL_ZERO_BLKS_RESULT
done


#write the result files to HDFS

#cleanup
sudo -u hdfs hdfs dfs -rmr -skipTrash  $HDFS_OUTPUT_DIR/allzeroblocks/$HOSTNAME
sudo -u hdfs hdfs dfs -rmr -skipTrash  $HDFS_OUTPUT_DIR/blocktimestamps/$HOSTNAME

#setup output dirs
sudo -u hdfs hdfs dfs -mkdir $HDFS_OUTPUT_DIR
sudo -u hdfs hdfs dfs -mkdir $HDFS_OUTPUT_DIR/allzeroblocks
sudo -u hdfs hdfs dfs -mkdir $HDFS_OUTPUT_DIR/blocktimestamps

#write the files to hdfs
sudo -u hdfs hdfs dfs -put $TMP_ALL_ZERO_BLKS_RESULT $HDFS_OUTPUT_DIR/allzeroblocks/$HOSTNAME
sudo -u hdfs hdfs dfs -put $TMP_ALL_BLK_PATHS $HDFS_OUTPUT_DIR/blocktimestamps/$HOSTNAME
