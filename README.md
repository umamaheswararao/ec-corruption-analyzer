# EC Corruption Analyzer

git clone https://github.com/umamaheswararao/ec-corruption-analyzer.git

cd ec-corruption-analyzer

mvn clean install

we can find the ec-corruption-analyzer-1.0-SNAPSHOT.jar generated in target folder.

We can analyze the EC corruption in two step process.

## Step1: 
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

## Step2:
After the step1, we should have generated all the required blocks meta information to run the analyzer tool.

Use any of hadoop node to run this tool where same version of hadoop installed and fs.defaultFS pointing to the same cluster where /scanning folder exist.
Copy the ec-corruption-analyzer-1.0-SNAPSHOT.jar to a directory <JAR_LOCATION>

Now run the following command:

./hadoop jar <JAR_LOCATION>/ec-corruption-analyzer-1.0-SNAPSHOT.jar org.apache.hadoop.hdfs.tools.ECCorruptFilesAnalyzer /scanning <ECDirs:EC directories list with comma separated> <output:Output directory where the analyzer results needs to be stored>
  
Here output can be local file system path as well. If you want to store in local file system, please make sure to give the output path prefixed with scheme. Ec: file://user/ec-analysis


