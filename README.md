# EC Corruption Analyzer

git clone https://github.com/umamaheswararao/ec-corruption-analyzer.git

Please refer BUILDING.txt file at project root folder and generate the package.

## Usage

We can analyze the EC corruption in two step process.

### Step1: 
We need to generate the block timestamps and allzero blocks from all datanode.
To generate that information, please use the scripts/EC_Blks_TimeStamp_And_AllZeroBlks_Finder.sh to generate the required information.
The above script needs list of data directories with comma separated as input and you need to run this in every datanode. 
You can use your prefered way(ex: ansible) to run this script distributedly in all DNs.

The script will write the blocktime stamps with blockpaths and also find allzero byte array blocks. From each DN, this script writes the scanned output in common HDFS location(/scanning).

The output structure would be in the following:
   /scanning/allzeroblocks/DN1_HOSTNAME
                          /DN2_HOSTNAME
                          /DN3_HOSTNAME 
                          .............
                          
   /scanning/blocktimestamps/DN1_HOSTNAME
                            /DN2_HOSTNAME
                            /DN3_HOSTNAME 
                            .............

### Step2:
After the step1, we should have generated all the required blocks meta information to run the analyzer tool.

Use any of hadoop node to run this tool where same version of hadoop installed and fs.defaultFS pointing to the same cluster where /scanning folder exist.
Copy the ec-corruption-analyzer-1.0-SNAPSHOT.jar to a directory <JAR_LOCATION>

Now run the following command:

./hadoop jar <JAR_LOCATION>/ec-corruption-analyzer-1.0-SNAPSHOT.jar org.apache.hadoop.hdfs.tools.ECCorruptFilesAnalyzer /scanning <ECDirs:EC directories list with comma separated> <output:Output directory where the analyzer results needs to be stored>
  
Here output can be local file system path as well. If you want to store in local file system, please make sure to give the output path prefixed with scheme. Ec: file://user/ec-analysis

The output folder contains 3 category of results. 
In the output directory:
1. ConsolidatedResult contains the all impacted files with the impacted specific block group information.
   
   example content:
   
   {
  "fileName" : "/TestECCorruptionAnalyzer/ECCorruptFileAnalyzer-recoverable",
  "policyName" : "RS-6-3-1024k",
  "blockGroupSize" : 9,
  "blockGroups" : [ {
    "blocks" : [ {
      "blockPath" : "/Users/umagangumalla/Work/repos/PRs/ec-corruption-analyzer/target/test/data/dfs/data/data23/current/BP-485120788-127.0.0.1-1609834596216/current/finalized/subdir0/subdir0/blk_-9223372036854775791",
      "locations" : [ "127.0.0.1:52620" ],
      "timeStamp" : 1609834903742,
      "isAllZeros" : false,
      "isParity" : false,
      "isMissingBlock" : false
    }, {
      "blockPath" : "/Users/umagangumalla/Work/repos/PRs/ec-corruption-analyzer/target/test/data/dfs/data/data11/current/BP-485120788-127.0.0.1-1609834596216/current/finalized/subdir0/subdir0/blk_-9223372036854775770",
      "locations" : [ "127.0.0.1:52578" ],
      "timeStamp" : 1609834904049,
      "isAllZeros" : true,
      "isParity" : true,
      "isMissingBlock" : false
    } ]
  } ]
}

2. UnrecoverableBlkGrpResult: this file contains the details of files which needs further attentions in recovery process as potential impacted files are more than or equal to parity.

3. RecoverableBlockGrpBlockPaths: this file contains sevaral files with block paths. If the blockGroup impacted blocks are less than or equal to parity, then we can safely move out the blocks from datanode directories. So, that HDFS will reconstruct them back with good data. Each file names with datanode IP@Port.txt format. Using rename_blks_with_locations.sh, can can move the blocks out from datanode directories.

Example file block content:
cat RecoverableBlockGrpBlockPaths/127.0.0.1@52022.txt
/Users/umagangumalla/Work/repos/PRs/ec-corruption-analyzer/target/test/data/dfs/data/data1/current/BP-2058363885-127.0.0.1-1609834454572/current/finalized/subdir0/subdir0/blk_-9223372036854775791
/Users/umagangumalla/Work/repos/PRs/ec-corruption-analyzer/target/test/data/dfs/data/data2/current/BP-2058363885-127.0.0.1-1609834454572/current/finalized/subdir0/subdir0/blk_-9223372036854775759




