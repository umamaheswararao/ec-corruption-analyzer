#!/bin/bash

blk_input_file=`hostname -s`
inode_input_file=`hostname -s`_inode
input_file=inodes_to_be_repaired
is_run=$1
stdout=stdout_`hostname -s`_inode_repair_p1_`date +"%m-%d-%Y-%H-%M-%S-%s"`
stderr=stderr_`hostname -s`_inode_repair_p1_`date +"%m-%d-%Y-%H-%M-%S-%s"`
set -x

while IFS= read -r line
do
	inode=$line
  	echo "processing inode: $inode" >> $stdout
	for blkid in `grep -w $inode $inode_input_file |awk -F ',' '{print $2}'`
	do
		blk_name=blk_$blkid
		echo "processing block name: $blk_name"  >> $stdout
		blk_location=`grep -w $blk_name $blk_input_file|awk -F ',' '{print $2}'`
		dirname=`dirname $blk_location`
  		blk_file=`basename $blk_location`
  		blk_metafile_path=`ls $blk_location*.meta`
		blk_metafile=`basename $blk_metafile_path`
		if [ "$is_run" != "run" ]
		then
			echo mock: mv $dirname/$blk_file $dirname/original_reconst_$blk_file
			echo mock: mv $dirname/$blk_metafile $dirname/original_reconst_$blk_metafile
		else
			to_be_moved_blk_file=$dirname/$blk_file
			to_be_moved_blk_metafile=$dirname/$blk_metafile
			if [[ "$to_be_moved_blk_file" == *BP* ]]
			then
				mv $dirname/$blk_file $dirname/original_reconst_$blk_file
				if [ $? == 1 ]
				then
					echo "Failed to rename $dirname/$blk_file" >> $stderr
				else
					echo "Moved $dirname/$blk_file TO $dirname/original_reconst_$blk_file" >> $stdout
				fi
				mv $dirname/$blk_metafile $dirname/original_reconst_$blk_metafile
				if [ $? == 1 ]
				then
					echo "Failed to rename $dirname/$blk_metafile" >> $stderr
				else
					echo "Moved $dirname/$blk_metafile TO $dirname/original_reconst_$blk_file" >> $stdout
				fi
			else
				echo "Seems like not a valid blk file/meta. Skipping $blkid" >> $stderr
			fi
		fi
	done
done < $input_file
set +x
