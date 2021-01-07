input_file=`hostname -i`
is_run=$1
timestring=`date +"%m-%d-%Y-%H-%M-%S-%s"`
stdout=stdout_`hostname -s`_`hostname -i`_inode_repair_p1_$timestring
stderr=stderr_`hostname -s`_`hostname -i`_inode_repair_p1_$timestring
while IFS= read -r line
do
  	echo "processing block file $line"
	dirname=`dirname $line`
  	blk_file=`basename $line`
  	blk_metafile_path=`ls $line*.meta`
	blk_metafile=`basename $blk_metafile_path`
	set -x
	if [ "$is_run" != "run" ]
		then
			echo mock: mv $dirname/$blk_file $dirname/original_reconst_$blk_file
			echo mock: mv $dirname/$blk_metafile $dirname/original_reconst_$blk_metafile
		else
			to_be_moved_blk_file=$dirname/$blk_file
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
				echo "Seems like not a valid blk file/meta. Skipping $to_be_moved_blk_file" >> $stderr
			fi
		fi

	set +x
done < $input_file
