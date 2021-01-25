# EC Corruption Analyzer

git clone https://github.com/umamaheswararao/ec-corruption-analyzer.git

Please refer BUILDING.txt file at project root folder and generate the package.

## Usage

We can analyze the EC corruption in two step process.

### Step1: Gather EC block data across the cluster
This step involves running a script on each DataNode. The script generates block timestamps and allzero blocks from all datanodes. To generate that information, please use the script at ec-corruption-analyzer-root>/scripts/EC_Blks_TimeStamp_And_AllZeroBlks_Finder.sh. The above script needs a comma-separated data directories list. The users can use their prefered way (e.g.: ansible) to run this script on all the DNs in the cluster.
The script will write its output into HDFS. Please make sure to kinit as an hdfs user in secured environments, because this script writes output files into hdfs. The output is two files for each DN. 
The first file has block timestamps and block file paths for blocks whose contents are all zeros. From each DN, this script writes the scanned output in common HDFS location(/scanning). <br/>
The output structure would be in the following: <br/>
<pre>
/scanning/allzeroblocks/DN1_HOSTNAME
                       /DN2_HOSTNAME
                       /DN3_HOSTNAME
                       ............. 
</pre>
The second file simply contains the timestamp and full path for each Erasure Coded block on the DataNode.
<pre>
/scanning/blocktimestamps/DN1_HOSTNAME
                         /DN2_HOSTNAME 
                         /DN3_HOSTNAME 
                         .............
</pre>

### Step2:Run Block Analyzer Tool
After step1, we have all the required blocks meta information to run the analyzer tool.
This tool can be run on any node where the same version of hadoop is installed and fs.defaultFS points to the same cluster where the /scanning folder exists.
 
Copy the ec-corruption-analyzer-1.0-SNAPSHOT.jar to a directory <JAR_LOCATION>
Now run the following command:
./hadoop jar <JAR_LOCATION>/ec-corruption-analyzer-1.0-SNAPSHOT.jar org.apache.hadoop.hdfs.tools.ECCorruptFilesAnalyzer <blockStatsPath> <targetPaths> <output>
 
#### Parameters:
##### blockStatsPath: 
An HDFS path where the gathered DataNodes block information stored. By default the gathered block information is stored at the directory /scanning in HDFS.
##### targetPaths:
A comma-separated list of HDFS paths to scan for the EC files and to identify the corrupted blocks. If you know the EC policy enabled paths, please give all of those paths by comma separated. If you are unaware of the EC enabled paths, then simply give HDFS root path ( that is / ). 
If you want to keep all the target paths in a separated local file and want to give that file as input, please use “targetPathsFile=” prefix. E.g: the second argument can be like targetPathsFile=/mylocaldir/input.txt
Here input.txt can have multiple paths and keep each path as a separated line in the file.
##### output:
A path where to store the output results from the EC Corruption Analyzer tool.
Here output can be a local file system path as well. If you want to store in a local file system, please make sure to give the output path prefixed with scheme. E.g.: file://user/ec-analysis

This tool loads all the block information(by reading from /scanning folder) into memory. 
This will lead to high memory usage and we recommend to set the little high heap for this tool.
<pre>
Please use the following way to set the heap memory.
  export HADOOP_CLIENT_OPTS="-Xmx128g $HADOOP_CLIENT_OPTS"
</pre>
 
After successfully running this tool, the output folder contains 3 categories of results. In the output directory:
#### ConsolidatedResult:
This file contains the all impacted files with the specific impacted block group information. We used json format for this file.
example content:
<pre>
{ "fileName" : "/TestECCorruptionAnalyzer/ECCorruptFileAnalyzer-recoverable", "policyName" : "RS-6-3-1024k",
"blockGroupSize" : 9,
"blockGroups" : [ { "blocks" : [ 

 { "blockPath" : "/Users/umagangumalla/Work/repos/PRs/ec-corruption-analyzer/target/test/data/dfs/data/data23/current/BP-485120788-127.0.0.1-1609834596216/current/finalized/subdir0/subdir0/blk_-9223372036854775791", 
"locations" : [ "127.0.0.1:52620" ],
 "timeStamp" : 1609834903742,
 "isAllZeros" : false,
 "isParity" : false,
 "isMissingBlock" : false },

 { "blockPath" : "/Users/umagangumalla/Work/repos/PRs/ec-corruption-analyzer/target/test/data/dfs/data/data11/current/BP-485120788-127.0.0.1-1609834596216/current/finalized/subdir0/subdir0/blk_-9223372036854775770",
 "locations" : [ "127.0.0.1:52578" ],
 "timeStamp" : 1609834904049,
 "isAllZeros" : true,
 "isParity" : true,
 "isMissingBlock" : false } ] } ] }
 </pre>

#### UnrecoverableBlkGrpResult:
This file contains the details of files which need further attention in the recovery process as potential impacted blocks in a blockGroup are more than or equal to parity. So, the format is pretty much the same as above, but it contains only the files which have more than or equal to parity blocks corrupted. Please note this identification of corruption is not exact as the identification is depending on timestamp comparisons. We will compare the last oldest block timestamp against the remaining blocks. If all blocks are too far from the oldest block, then we assume all of these blocks are reconstructed and potentially impacted. Since this tool is depending timestamps of the blocks, running balancer in cluster could impact the results and you may notice more results in this file. That is because all of the modified timestamp blocks will appear as suspected blocks. 

#### RecoverableBlockGrpBlockPaths:
This file contains several files with block paths. If the blockGroup impacted blocks are less than or equal to parity, then we can safely move out the blocks from datanode directories. So, HDFS will reconstruct them back with good data. Each file names with datanode IP. Using rename_blks_with_locations.sh, can move the blocks out from datanode directories.
<pre>
cat RecoverableBlockGrpBlockPaths/127.0.0.1 /Users/umagangumalla/Work/repos/PRs/ec-corruption-analyzer/target/test/data/dfs/data/data1/current/BP-2058363885-127.0.0.1-1609834454572/current/finalized/subdir0/subdir0/blk_-9223372036854775791 /Users/umagangumalla/Work/repos/PRs/ec-corruption-analyzer/target/test/data/dfs/data/data2/current/BP-2058363885-127.0.0.1-1609834454572/current/finalized/subdir0/subdir0/blk_-9223372036854775759
</pre>


## Step3: Rename the recoverable blocks to let HDFS reconstruct them back 
With the help of rename_blks_with_locations.sh script , we would rename all block/meta files in each DN. 
Please use ansible script or any other way depending on your preference to run the below script in each DN. 
<pre>
./rename_blks_with_locations → no args to perform dry run in all DataNodes
./rename_blks_with_locations run → To actually rename blocks
</pre>
 
## Step4: Restart DataNodes
Restart DataNodes that are participating in this rename operation.
Tip: Check the RecoverableBlockGrpBlockPaths folder and under this folder file names supposed to be with hostname. So, of those filenames matching hosts needed the restart. 


## Downloads
The EC Corruption Analyzer tool can be downloaded at the following links:
      https://github.com/umamaheswararao/ec-corruption-analyzer/releases/

Please note that you may need to download the version against the hadoop version which was used in compilation dependency. 

Example: if the analyzer tool version is 1.0-3.0.0-cdh-6.3.4, then that release would have been compiled with 3.0.0-cdh-6.3.4. 
If you want to compile from the source, please clone the repo (https://github.com/umamaheswararao/ec-corruption-analyzer.git ) and refer to BUILDING.txt file for build instructions.
