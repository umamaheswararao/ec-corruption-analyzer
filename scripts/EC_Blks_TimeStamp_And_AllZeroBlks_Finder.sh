#!/bin/bash
#provide all data dir paths by comma separated
#DATA_DIRS=/dataroot/ycloud,/dataroot/ycloud1
DATA_DIRS=$1

TMP_ALL_ZERO_BLKS_RESULT=/tmp/all_zero_blks_result$$.txt
TMP_ALL_BLK_PATHS_WITH_TIMESTAMP_RESULT=/tmp/all_blk_paths_result$$.txt
HDFS_OUTPUT_DIR=/scanning/


rm -rf $TMP_ALL_ZERO_BLKS_RESULT
rm -rf $TMP_ALL_BLK_PATHS_WITH_TIMESTAMP_RESULT

#Scans for the block timestamps and  all zeros blocks
findBlkTimeStampAndAllZeroBlocksInDir(){
  echo "Scanning dir:" $1

  for blockPath in $(find $1 -type f -iname "*blk_*" | awk '!/.meta/{print }' | awk '/BP-/{print}')
  do
      #echo The block path is $blockPath
      BLK_PATH_WITH_TIME_STAMP=$(stat -c '%.9Y %n' $blockPath)
      echo $BLK_PATH_WITH_TIME_STAMP >>$TMP_ALL_BLK_PATHS_WITH_TIMESTAMP_RESULT

      #Find if it's a allzero block
      compare=$(cmp  $blockPath /dev/zero)
      if [[ $compare = "" ]]; then echo $blockPath | uniq >>$TMP_ALL_ZERO_BLKS_RESULT; fi
  done
}

# Loop over to all data directories and call findBlkTimeStampAndAllZeroBlocksInDir function for finding timestamp and all zero blocks
for DATADIR in $(echo $DATA_DIRS | sed "s/,/ /g")
do
    findBlkTimeStampAndAllZeroBlocksInDir "$DATADIR"
done

echo "Scanning finished"


#write the result files to HDFS

#cleanup
echo "Trying to cleanup the old result files"
hdfs dfs -rm -r -skipTrash  $HDFS_OUTPUT_DIR/allzeroblocks/$HOSTNAME
hdfs dfs -rm -r -skipTrash  $HDFS_OUTPUT_DIR/blocktimestamps/$HOSTNAME

#setup output dirs
hdfs dfs -mkdir -p $HDFS_OUTPUT_DIR
hdfs dfs -mkdir -p $HDFS_OUTPUT_DIR/allzeroblocks
hdfs dfs -mkdir -p $HDFS_OUTPUT_DIR/blocktimestamps

#write the files to hdfs
echo "Uploading the block timestamp and allzero blocks scanning results to hdfs"
hdfs dfs -put $TMP_ALL_ZERO_BLKS_RESULT $HDFS_OUTPUT_DIR/allzeroblocks/$HOSTNAME
hdfs dfs -put $TMP_ALL_BLK_PATHS_WITH_TIMESTAMP_RESULT $HDFS_OUTPUT_DIR/blocktimestamps/$HOSTNAME

echo "Uploaded the results to HDFS location at $HDFS_OUTPUT_DIR"