/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * This class starts and runs EC Block Validation service.
 */
public class ECCorruptFilesAnalyzer {
  public static final Logger LOG =
      LoggerFactory.getLogger(ECCorruptFilesAnalyzer.class);
  private static final int SAFE_BLK_RENAME_PER_NODE_FILE_FLUSH_NUM = 15;
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static ECBlockStatsProvider stats = new ECBlockStatsProvider();
  private static long EXPECTED_TIME_GAP_BETWEEN_FILE_AND_BLOCKS = 5 * 60 * 1000;
  private static long EXPECTED_TIME_GAP_BETWEEN_OLDEST_BLK_AND_OTHER_BLOCKS = 5 * 1000;
  static String ALL_ZEROS_BLOCKS_FOLDER = "allzeroblocks";
  static String BLOCK_TIME_STAMPS_FOLDER = "blocktimestamps";
  private static long expected_time_gap_between_inode_and_blocks =
      EXPECTED_TIME_GAP_BETWEEN_FILE_AND_BLOCKS;
  private static long expected_time_gap_between_oldest_blk_and_other_blks =
      EXPECTED_TIME_GAP_BETWEEN_OLDEST_BLK_AND_OTHER_BLOCKS;
  private static boolean NEED_SECURE_LOGIN = false;
  private static boolean needSecureLogin = false;
  private static boolean CHECK_AGAINST_INODE_TIME = false;
  private static boolean checkAgainstInodeTime = CHECK_AGAINST_INODE_TIME;
  private static boolean CHECK_ALL_ZEROS_DEFAULT = true;
  private static boolean checkAllZeros = CHECK_ALL_ZEROS_DEFAULT;
  private static Map<String, ErasureCodingPolicy> ecNameVsPolicy = new HashMap<>();
  private ResultsProcessor results;


  public static void initStats(Path ecBlockStatsPath, Configuration conf)
      throws IOException, URISyntaxException {
    stats.init(ecBlockStatsPath, conf);
  }

  private static List<Path> readPathFile(String file) throws IOException {
    List<Path> list = com.google.common.collect.Lists.newArrayList();
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(new FileInputStream(file), "UTF-8"));
    try {
      String line;
      while ((line = reader.readLine()) != null) {
        if (!line.trim().isEmpty()) {
          list.add(new Path(line));
        }
      }
    } finally {
      IOUtils.cleanup(null, reader);
    }
    return list;
  }

  /**
   * Main method to start validating service service.
   */
  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println(
          "Not sufficient arguments provided. Expected parameters are \n" +
              " 1. statsPath - where scripts/EC_Blks_TimeStamp_And_AllZeroBlks_Finder.sh generated outputs,\n" +
              " 2. list of target paths to scan for EC files. \n" +
              " 3. Output directory path.");
      return;
    }

    Path ecBlockStatsPath = new Path(args[0]);
    List targetPaths = new ArrayList();//Path[split.length];
    String targetPathArg = args[1];
    if (targetPathArg.startsWith("targetPathsFile=")) {
      targetPaths = readPathFile(targetPathArg
          .substring("targetPathFile=".length() - 1, targetPathArg.length()));
    } else {
      String split[] = args[1].split(",");
      for (int i = 0; i < split.length; i++) {
        targetPaths.add(new Path(split[i]));
      }
    }

    Path outPath = args.length > 2 ? new Path(args[2]) : null;
    HdfsConfiguration conf = new HdfsConfiguration();
    needSecureLogin =
        conf.getBoolean("ec.analyzer.need.secure.login", NEED_SECURE_LOGIN);
    checkAllZeros = conf.getBoolean("ec.analyzer.check.all.zero.blocks",
        CHECK_ALL_ZEROS_DEFAULT);
    expected_time_gap_between_inode_and_blocks =
        conf.getLong("ec.analyzer.expected.time.gap.between.file.and.blks",
            EXPECTED_TIME_GAP_BETWEEN_FILE_AND_BLOCKS);
    expected_time_gap_between_oldest_blk_and_other_blks =
        conf.getLong("ec.analyzer.expected.time.gap.between.oldest.blk.and.other.blks",
            EXPECTED_TIME_GAP_BETWEEN_OLDEST_BLK_AND_OTHER_BLOCKS);
    checkAgainstInodeTime = conf.getBoolean("ec.analyzer.check.against.inode.time", CHECK_AGAINST_INODE_TIME);

    System.out.println(
        "##############################################################################################");
    System.out.println(
        "#########   Loaded Configurations                                                  ");
    System.out.println(
        "#########   ec.analyzer.check.all.zero.blocks = " + checkAllZeros);
    System.out.println(
        "#########   ec.analyzer.expected.time.gap.between.file.and.blks = " + expected_time_gap_between_inode_and_blocks);
    System.out.println(
        "#########   ec.analyzer.expected.time.gap.between.oldest.blk.and.other.blks = " + expected_time_gap_between_oldest_blk_and_other_blks);
    System.out.println(
        "#########   ec.analyzer.check.against.inode.time = " + checkAgainstInodeTime);
    System.out.println(
        "##############################################################################################");


    //For now just use balancer key tab
    if (needSecureLogin) {
      secureLogin(conf);
    }
    ECCorruptFilesAnalyzer analyzer = new ECCorruptFilesAnalyzer();
    analyzer.analyze(ecBlockStatsPath, targetPaths, outPath, conf);
  }

  public void analyze(Path ecBlockStatsPath, List<Path> targetPaths, Path outPath,
      Configuration conf)
      throws IOException, URISyntaxException, InterruptedException {
    DistributedFileSystem dfs = new DistributedFileSystem();
    results = new ResultsProcessor(outPath, conf);
    try {
      dfs.initialize(FileSystem.getDefaultUri(conf), conf);
      for (ErasureCodingPolicyInfo ecInfo : dfs.getAllErasureCodingPolicies()) {
        ecNameVsPolicy.put(ecInfo.getPolicy().getName(), ecInfo.getPolicy());
      }
      System.out.println("##############################################################################################");
      System.out.println("#########  Loading the block time stamps and allZeros block details into memory   ############");
      System.out.println("##############################################################################################");
      initStats(ecBlockStatsPath, conf);
      System.out.println("##############################################################################################");
      System.out.println("#########  Loading of the block time stamps and allZeros block details finished   ############");
      System.out.println("##############################################################################################");
      results.start();
      System.out.println("##############################################################################################");
      System.out.println("#########            Crawling HDFS target paths for checking the corruption       ############");
      System.out.println("##############################################################################################");
      processNamespace(targetPaths, dfs, results);
    } finally {
      results.stopProcessorGracefully();
      dfs.close();
      System.out.println("##############################################################################################");
      System.out.println("#########                             Analysis Finished.                          ############");
      System.out.println("#########   Finished. Please check the results at the location : " +outPath+ "       ############");
      System.out.println("##############################################################################################");
    }
  }

  public static void secureLogin(Configuration conf) throws IOException {
    UserGroupInformation.setConfiguration(conf);
    String addr = conf.get(DFSConfigKeys.DFS_BALANCER_ADDRESS_KEY,
        DFSConfigKeys.DFS_BALANCER_ADDRESS_DEFAULT);
    InetSocketAddress socAddr = NetUtils
        .createSocketAddr(addr, 0, DFSConfigKeys.DFS_BALANCER_ADDRESS_KEY);
    SecurityUtil.login(conf, DFSConfigKeys.DFS_BALANCER_KEYTAB_FILE_KEY,
        DFSConfigKeys.DFS_BALANCER_KERBEROS_PRINCIPAL_KEY,
        socAddr.getHostName());
  }

  public static void processNamespace(List<Path> targetPaths,
      DistributedFileSystem dfs, ResultsProcessor results) throws IOException {
    for (Path target : targetPaths) {
      processPath(target.toUri().getPath(), dfs, results);
    }
  }

  /**
   * @return whether there is still remaing migration work for the next
   * round
   */
  private static void processPath(String fullPath, DistributedFileSystem dfs,
      ResultsProcessor results) {
    DFSClient client = dfs.getClient();
    for (byte[] lastReturnedName = HdfsFileStatus.EMPTY_NAME; ; ) {
      final DirectoryListing children;
      try {
        children = client.listPaths(fullPath, lastReturnedName, true);
      } catch (IOException e) {
        LOG.warn(
            "Failed to list directory " + fullPath + ". Ignore the directory and continue.",
            e);
        return;
      }
      if (children == null) {
        return;
      }
      for (HdfsFileStatus child : children.getPartialListing()) {
        processRecursively(fullPath, child, dfs, results);
      }
      if (children.hasMore()) {
        lastReturnedName = children.getLastName();
      } else {
        return;
      }
    }
  }

  /**
   * @return whether the migration requires next round
   */
  private static void processRecursively(String parent, HdfsFileStatus status,
      DistributedFileSystem dfs, ResultsProcessor results) {
    String fullPath = status.getFullName(parent);
    if (status.isDirectory()) {
      if (!fullPath.endsWith(Path.SEPARATOR)) {
        fullPath = fullPath + Path.SEPARATOR;
      }
      processPath(fullPath, dfs, results);
    } else if (!status.isSymlink()) { // file
      try {
        if (!isSnapshotPathInCurrent(fullPath, dfs)) {
          //the full path is a snapshot path but it is also included in the
          // current directory tree, thus ignore it.
          processFile(fullPath, (HdfsLocatedFileStatus) status, dfs, results);
        }
      } catch (IOException e) {
        LOG.warn(
            "Failed to check the status of " + parent + ". Ignore it and continue.",
            e);
      }
    }
  }

  private static void processFile(String fullPath, HdfsLocatedFileStatus status,
      DistributedFileSystem dfs, ResultsProcessor results) {
    final LocatedBlocks locatedBlocks = status.getBlockLocations();
    long inodeModificationTime = status.getModificationTime();
    final ErasureCodingPolicy ecPolicy = locatedBlocks.getErasureCodingPolicy();

    if (ecPolicy != null) { //Found EC file
      LOG.debug("Found EC file to scan:" + fullPath);
      int totalBlockGrpNum =
          ecPolicy.getNumDataUnits() + ecPolicy.getNumParityUnits();
      final int cellSize = ecPolicy.getCellSize();
      final int dataBlkNum = ecPolicy.getNumDataUnits();
      final int parityBlkNum = ecPolicy.getNumParityUnits();
      List<BlockGroup> bgCorruptBlks = new ArrayList<>();
      // Scan all block groups in this file
      for (LocatedBlock firstBlock : locatedBlocks.getLocatedBlocks()) {
        LocatedBlock[] blocks = StripedBlockUtil
            .parseStripedBlockGroup((LocatedStripedBlock) firstBlock, cellSize,
                dataBlkNum, parityBlkNum);
        //check if any internal block has allZeros
        List<LocatedBlock> allZeroBlks = new ArrayList<>();
        PriorityQueue<Block> pq =
            new PriorityQueue<>(new Comparator<Block>() {
              @Override
              public int compare(Block o1, Block o2) {
                return o1.getTimeStamp().compareTo(o2.getTimeStamp());
              }
            });
        long oldestBlockTime = Long.MAX_VALUE;
        for (int i=0; i< blocks.length; i++) {
          LocatedBlock block = blocks[i];
          if (block == null) {
            System.out.println(
                "Block location is not reported to NN by any DN. So, ignoring this block from analysis. The block is:" + new org.apache.hadoop.hdfs.protocol.Block(
                    firstBlock.getBlock()
                        .getBlockId() + i) + " and the file name is: " + fullPath);
            continue;
          }
          String[] locs =
              Arrays.stream(block.getLocations()).map(datanodeInfo -> {
                return datanodeInfo.getIpAddr();
              }).toArray(String[]::new);
          boolean isParity = isParityBlock(
              block.getBlock(), dataBlkNum);
          Long modifiedTime = stats.getModifiedTime(block.getBlock());
          String blockWithPath = stats.getBlockWithPath(block.getBlock());

          oldestBlockTime = Math.min(modifiedTime, oldestBlockTime);
          if (stats.allZeroBlockIds
              .contains(block.getBlock()) && isParity) { //Currently
            allZeroBlks.add(block);
            pq.offer(
                new Block(block.getBlock(), blockWithPath, locs, modifiedTime,
                    true, isParity, false));
          } else {
            //TODO:// if no blocks in DNs, then modified time is Long.MAX and path is ""
            boolean isMissingBlock =
                "".equals(blockWithPath) && modifiedTime == Long.MAX_VALUE;
            pq.offer(
                new Block(block.getBlock(), blockWithPath, locs, modifiedTime,
                    false, isParity, isMissingBlock));
          }

        }
        if (allZeroBlks.size() > 0 && checkAllZeros) { // Found all zero blocks
          //Find first created zero block
          LocatedBlock firstAllZeroBlk = allZeroBlks.get(0);
          long firstZeroBlkTime =
              stats.getModifiedTime(firstAllZeroBlk.getBlock());
          for (int i = 1; i < allZeroBlks.size(); i++) {
            LocatedBlock blk = allZeroBlks.get(i);
            long currZeroBlkTime = stats.getModifiedTime(blk.getBlock());
            if (firstZeroBlkTime > currZeroBlkTime) {
              firstZeroBlkTime = stats.getModifiedTime(blk.getBlock());
            }
          }

          //remove all blocks created prior to firstAllZeroBlock created
          while (!pq.isEmpty()) {
            Block peek = pq.peek();
            if (peek.getTimeStamp() < firstZeroBlkTime) {
              pq.remove();
            } else {
              break;
            }
          }
          bgCorruptBlks.add(
              new BlockGroup(Lists.newArrayList(pq.iterator()),
                  BlockGroup.Corruption_Type.ALL_ZEROS_WITH_ADDITIONAL_BLOCKS));
        } else { //No allZero blocks, but lets check the time variation between
          // blocks and file modification time.
          // If appends used, time variation could be high or it could go wrong.
          List<Block> possibleCorruptions = new ArrayList<>();
          while (!pq.isEmpty()) {
            Block nextCreatedBlk = pq.remove();
            if (checkAgainstInodeTime ?
                checkWithInodeTime(inodeModificationTime,
                    nextCreatedBlk.timeStamp) :
                checkWithOldestBlk(oldestBlockTime, nextCreatedBlk.timeStamp)) {
              possibleCorruptions.add(nextCreatedBlk);
            }
          }

          if (possibleCorruptions.size() > 0) {
            bgCorruptBlks.add(new BlockGroup(possibleCorruptions,
                BlockGroup.Corruption_Type.MORE_VARIED_TIME_STAMPS));
          }
        }
      }

      if (bgCorruptBlks.size() > 0) {
        results.addToResult(fullPath,totalBlockGrpNum, bgCorruptBlks, ecPolicy.getName());
      }
    }
  }

  private static boolean checkWithInodeTime(long inodeModificationTime,
      long currentBlkTime) {
    return Math.abs(
        currentBlkTime - inodeModificationTime) > expected_time_gap_between_inode_and_blocks;
  }

  private static boolean checkWithOldestBlk(long oldestBlkTime,
      long currentBlkTime) {
    return Math.abs(
        currentBlkTime - oldestBlkTime) > expected_time_gap_between_oldest_blk_and_other_blks;
  }

  private static boolean isParityBlock(ExtendedBlock block, int i) {
    return StripedBlockUtil.getBlockIndex(block.getLocalBlock()) >= i;
  }

  public Map<String, List<BlockGroup>> getResults(){
    return this.results.results;
  }

  public void stop() throws IOException, InterruptedException {
    if(this.results!=null) {
      this.results.stopProcessorGracefully();
    }
  }

  /**
   * ECBlockStatsProvider loads the stats files generated by all datanodes.
   */
  static class ECBlockStatsProvider {
    FileSystem fs;
    // <ECBlockStatsDir>/allzeroblocks/dnhost
    // <ECBlockStatsDir>/blocktimestamps/dnhost
    private List<ExtendedBlock> allZeroBlockIds = new ArrayList<>();
    private Map<ExtendedBlock, Stats> blockVsModifiedTime = new HashMap<>();

    public void init(Path statsPath, Configuration conf) throws IOException {
      fs = FileSystem.get(statsPath.toUri(), conf);
      FileStatus[] fStatus = fs.listStatus(statsPath);
      if (fStatus.length < 2) {
        System.out.println(
            "Not enough data file generated. Please make sure, allZero blocks and blocksVsTimes stamps available.");
      }

      for (FileStatus status : fStatus) {
        Path file = status.getPath();
        if (ALL_ZEROS_BLOCKS_FOLDER.equals(file.getName())) {
          // load <ECBlockStatsDir>/allzeroblocks/
          loadAllZeroBlocks(file, fs);
        } else {
          if (BLOCK_TIME_STAMPS_FOLDER.equals(file.getName())) {
            //load <ECBlockStatsDir>/blocktimestamps/
            loadBlockTimeStamps(file, fs);
          }
        }
      }
    }

    private void loadBlockTimeStamps(Path file, FileSystem fsystem)
        throws IOException {
      FileStatus[] fStatus = fsystem.listStatus(file);
      for (FileStatus fs : fStatus) {
        Path f = fs.getPath();
        try (FSDataInputStream open = fsystem.open(f)) {
          BufferedReader br = new BufferedReader(new InputStreamReader(open));
          String line = br.readLine();
          while (line != null) {
            String[] splits = line.split("\\s+");
            if (splits.length < 2) {
              System.out.println(
                  "Wrong block data found in file: " + file + ". skipping this entry: " + line);
            }
            String timeStr = splits[0];
            String blockPathStr = splits[1];
            String blockPoolID = null;
            ExtendedBlock block = parseBlockString(blockPathStr);
            long currTimeInMillis = convert(timeStr);
            if (blockVsModifiedTime.containsKey(block)) {
              long oldTime = blockVsModifiedTime.get(block).time;
              currTimeInMillis = Math.max(currTimeInMillis,
                  oldTime); // picking the latest one for corruption validation.
            }
            //We can fill locations if we store them in stats files. But for now, we keep null here.
            blockVsModifiedTime.put(block,
                new Stats(currTimeInMillis, blockPathStr, false, null));

            line = br.readLine();
          }
        }
      }
    }

    private void loadAllZeroBlocks(Path file, FileSystem fsystem)
        throws IOException {
      FileStatus[] fstatus = fsystem.listStatus(file);
      for (FileStatus fs : fstatus) {
        Path f = fs.getPath();
        try (FSDataInputStream open = fsystem.open(f)) {
          BufferedReader br = new BufferedReader(new InputStreamReader(open));
          String line = br.readLine();
          while (line != null) {
            String blockIdStr = line;
            ExtendedBlock block = parseBlockString(blockIdStr);
            allZeroBlockIds.add(block);
            line = br.readLine();
          }
        }
      }
    }

    private ExtendedBlock parseBlockString(String blockStr) {
      //Extract blockFile name
      String split1[] = blockStr.split("/");
      String blockFileStr = split1[split1.length - 1];

      //Extract BP ID
      String split[] = blockStr.split("/BP-");
      String secondPart = split[1];

      String split3[] = secondPart.split("/");

      String bpID = "BP-" + split3[0];

      return new ExtendedBlock(bpID, org.apache.hadoop.hdfs.protocol.Block.filename2id(blockFileStr));
    }

    private long convert(String timeStr) {
      String[] split = timeStr.split("\\.");
      if (split.length < 1 || split.length > 2) {
        throw new IllegalArgumentException(
            "Unexpected time format. Time should be in millis or seconds.millis");
      }
      if (split.length == 1) {
        return Long.valueOf(split[0]);
      }

      long secsInMillis = TimeUnit.MILLISECONDS
          .convert(Long.valueOf(split[0]), TimeUnit.SECONDS);
      //Ignoring millis part split[1] for now as expected time gap is fairly larger than this.
      long timeStampInMillis = secsInMillis;
      return timeStampInMillis;
    }

    public boolean isInAllZerosBlock(ExtendedBlock blk) {
      return allZeroBlockIds.contains(blk);
    }

    public Long getModifiedTime(ExtendedBlock blk) {
      Stats stats = blockVsModifiedTime.get(blk);
      return stats!=null? stats.time: Long.MAX_VALUE;
    }

    public String getBlockWithPath(ExtendedBlock blk) {
      Stats stats = blockVsModifiedTime.get(blk);
      return stats != null ? stats.path : "";
    }
  }

  static class Stats {
    private final String[] locations;
    long time;
    String path;
    boolean isAllZeros;

    public Stats(long time, String path, boolean isAllZeros,
        String[] locations) {
      this.time = time;
      this.path = path;
      this.isAllZeros = isAllZeros;
      this.locations = locations;
    }

    @Override
    public String toString() {
      return "Stats{" + "time=" + time + ", path='" + path + '\'' + ", isAllZeros=" + isAllZeros + " locations: " + Arrays
          .toString(locations) + '}';
    }
  }

  public static class ResultsProcessor extends Thread {
    private Path consolidatedResultPath = null;
    private Path unrecoverableBlkGrpResultPath = null;
    private BufferedWriter bwForConsolidatedResultPath = null;
    private BufferedWriter bwForUnrecoverableBlkGrpResultPath = null;
    private Path outPutPath;
    private Path safeBlksToRenamePath;
    private FileSystem fs;

    public ResultsProcessor(Path outPutPath, Configuration conf) throws IOException {
      this.outPutPath = outPutPath;
      fs = this.outPutPath != null ? this.outPutPath.getFileSystem(conf) : null;
      this.safeBlksToRenamePath = this.outPutPath != null ?
          new Path(this.outPutPath, "RecoverableBlockGrpBlockPaths") :
          null;
      this.consolidatedResultPath = this.outPutPath != null ?
          new Path(this.outPutPath, "ConsolidatedResult") :
          null;
      if (this.consolidatedResultPath != null) {
        bwForConsolidatedResultPath = new BufferedWriter(
            new OutputStreamWriter(fs.create(this.consolidatedResultPath)));
      }
      this.unrecoverableBlkGrpResultPath = this.outPutPath != null ?
          new Path(this.outPutPath, "UnrecoverableBlkGrpResult") :
          null;
      if (this.unrecoverableBlkGrpResultPath != null) {
        bwForUnrecoverableBlkGrpResultPath = new BufferedWriter(
            new OutputStreamWriter(
                fs.create(this.unrecoverableBlkGrpResultPath)));
      }
    }

    private Map<String, List<BlockGroup>> results =
        new HashMap<>();
    private static volatile boolean running = false;

    private Queue<PossibleImpactedECFile> queue = new LinkedList<>();
    private Map<String, List<String>> safeBlocksToRename = new ConcurrentHashMap<>();

    public Map<String, List<BlockGroup>> getAllResults() {
      return this.results;
    }

    public synchronized void addToResult(String file, int blockGroupSize, List<BlockGroup> bgCorruptBlks, String policyName) {
      queue.offer(new PossibleImpactedECFile(file,policyName,blockGroupSize, bgCorruptBlks));
    }

    public synchronized PossibleImpactedECFile poll() {
      return queue.poll();
    }

    @Override
    public void run() {
      running = true;
      while(true) {
        PossibleImpactedECFile fileBlkGrps = poll();
        if (fileBlkGrps != null) {
          if (this.outPutPath == null) {
            // This is just in memory for testing.
            results
                .put(fileBlkGrps.getFileName(), fileBlkGrps.blockGroups);
            continue;
          }
          List<BlockGroup> blockGrpCorruptedBlocks =
              fileBlkGrps.getBlockGroups();

          List<BlockGroup> consolidatedBlkGrpJson = new ArrayList<>();
          List<BlockGroup> unrecoverableBlkGrpJson = new ArrayList<>();
          for (int i = 0; i < blockGrpCorruptedBlocks.size(); i++) {
            BlockGroup blkGrp = blockGrpCorruptedBlocks.get(i);
            if (blkGrp.getBlocks().size() <= ecNameVsPolicy
                .get(fileBlkGrps.getPolicyName()).getNumParityUnits()) {
              LOG.info(
                  "Found recoverable block group in file:" + fileBlkGrps
                      .getFileName() + " impacted blks : " + blkGrp
                      .getBlocks());
              for (Block blk : blkGrp.getBlocks()) {
                //In EC, do we get more than one locations returned?
                List<String> paths = safeBlocksToRename
                    .getOrDefault(blk.locations[0], new ArrayList<>());
                paths.add(blk.blockPath);
                safeBlocksToRename.put(blk.locations[0], paths);
              }
            } else {
              //unrecoverable block groups detected.
              List<Block> unrecoverableBlksJson = new ArrayList<>();
              for (Block blk : blkGrp.getBlocks()) {
                unrecoverableBlksJson.add(new Block(blk.block, blk.getBlockPath(),blk. getLocations(), blk. getTimeStamp(), blk. getIsAllZeros(), blk.getIsParity(), blk.isMissingBlock));
              }
              unrecoverableBlkGrpJson.add(new BlockGroup(unrecoverableBlksJson, blkGrp.type));

            }

            //consolidated
            List<Block> consolBlksJson = new ArrayList<>();
            for (Block blk : blkGrp.getBlocks()) {
              consolBlksJson.add(new Block(blk.block, blk.getBlockPath(),blk.getLocations(), blk. getTimeStamp(), blk.getIsAllZeros(), blk.getIsParity(), blk.isMissingBlock));
            }
            consolidatedBlkGrpJson.add(new BlockGroup(consolBlksJson, blkGrp.type));
          }

          if (unrecoverableBlkGrpJson.size() > 0) {
            try {
              bwForUnrecoverableBlkGrpResultPath.write(
                  MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(
                      new PossibleImpactedECFile(fileBlkGrps.getFileName(),fileBlkGrps.getPolicyName(), fileBlkGrps.getBlockGroupSize(), unrecoverableBlkGrpJson)));
              bwForUnrecoverableBlkGrpResultPath.newLine();
              bwForUnrecoverableBlkGrpResultPath.flush();
            } catch (IOException e) {
              LOG.error("Unable to write unrecoverable block groups details",
                  e);
            }
          }

          try {
            bwForConsolidatedResultPath.write(
                MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(
                    new PossibleImpactedECFile(fileBlkGrps.getFileName(),
                        fileBlkGrps.getPolicyName(), fileBlkGrps.getBlockGroupSize(),  consolidatedBlkGrpJson)));
            bwForConsolidatedResultPath.newLine();
            bwForConsolidatedResultPath.flush();
          } catch (IOException e) {
            LOG.error(
                "Unable to write consolidated impacted block groups details",
                e);
          }

          //check if we can flush to files
          if (safeBlocksToRename.size() > 0) {
            writeSafeToRenameBlockPaths(safeBlocksToRename,
                SAFE_BLK_RENAME_PER_NODE_FILE_FLUSH_NUM);
          }

        } else {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.info("Results processor thread interrupted. Exiting");
            break;
            //
          }
        }

        if(!running && poll() == null){ // scanning already done and called stop.
          break;
        }
      }
    }

    private synchronized void writeSafeToRenameBlockPaths(
        Map<String, List<String>> safeBlocksToRename, int flushNum) {
      if (safeBlocksToRename.size() <= 0) {
        return;
      }
      Iterator<Map.Entry<String, List<String>>> iter =
          safeBlocksToRename.entrySet().iterator();
      while(iter.hasNext()){
        Map.Entry<String, List<String>> next = iter.next();
        List<String> paths = next.getValue();
        if(paths.size()>flushNum){ //make it configurable
          //Let's flush
          if(fs!=null && safeBlksToRenamePath !=null){
            Path filePath = new Path(safeBlksToRenamePath, next.getKey());
            try {
              // Use cache for fos, otherwise it's inefficient.
              try (FSDataOutputStream fos = fs.exists(filePath) ?
                  fs.append(filePath) :
                  fs.create(filePath)) {
                BufferedWriter bw =
                    new BufferedWriter(new OutputStreamWriter(fos));
                for (int i = 0; i < paths.size(); i++) {
                  bw.write(paths.get(i));
                  bw.newLine();
                }
                bw.close();
              }

            } catch (IOException e) {
              LOG.error("Unable to write to safe to rename blocks path", e);
            }
          }//fs not available
          iter.remove();
        }
      }
    }

    public void stopProcessorGracefully()
        throws InterruptedException, IOException {
      while(!queue.isEmpty()){
        Thread.sleep(1000);
      }
      if(safeBlocksToRename.size()>0){
        writeSafeToRenameBlockPaths(safeBlocksToRename, 0);
      }
      running = false;

      if(bwForConsolidatedResultPath!=null){
        bwForConsolidatedResultPath.close();
      }

      if(bwForUnrecoverableBlkGrpResultPath !=null){
        bwForUnrecoverableBlkGrpResultPath.close();
      }

      this.interrupt();
    }

    @Override
    public String toString() {
      return "Results{" + "results=" + queue + '}';
    }
  }



  /**
   * @return true if the given path is a snapshot path and the corresponding
   * INode is still in the current fsdirectory.
   */
  private static boolean isSnapshotPathInCurrent(String path,
      DistributedFileSystem dfs) throws IOException {
    // if the parent path contains "/.snapshot/", this is a snapshot path
    if (path.contains(HdfsConstants.SEPARATOR_DOT_SNAPSHOT_DIR_SEPARATOR)) {
      String[] pathComponents = INode.getPathNames(path);
      if (HdfsConstants.DOT_SNAPSHOT_DIR
          .equals(pathComponents[pathComponents.length - 2])) {
        // this is a path for a specific snapshot (e.g., /foo/.snapshot/s1)
        return false;
      }
      String nonSnapshotPath = convertSnapshotPath(pathComponents);
      return dfs.getClient().getFileInfo(nonSnapshotPath) != null;
    } else {
      return false;
    }
  }

  private static String convertSnapshotPath(String[] pathComponents) {
    StringBuilder sb = new StringBuilder(Path.SEPARATOR);
    for (int i = 0; i < pathComponents.length; i++) {
      if (pathComponents[i].equals(HdfsConstants.DOT_SNAPSHOT_DIR)) {
        i++;
      } else {
        sb.append(pathComponents[i]);
      }
    }
    return sb.toString();
  }

  static class Block {
    private ExtendedBlock block;
    private String blockPath;
    private String[] locations;
    private long timeStamp;
    private boolean isAllZeros;
    private boolean isParity;
    private boolean isMissingBlock;

    public Block(ExtendedBlock block, String blkPath, String[] locations,
        long time, boolean isAllZeros, boolean isParity, boolean isMissingBlock) {
      this.block = block;
      this.blockPath = blkPath;
      this.locations = locations;
      this.timeStamp = time;
      this.isAllZeros = isAllZeros;
      this.isParity = isParity;
      this.isMissingBlock = isMissingBlock;
    }

    public void setBlockPath(String blockPath) {
      this.blockPath = blockPath;
    }

    public String getBlockPath() {
      return blockPath;
    }

    public void setTimeStamp(long time) {
      this.timeStamp = time;
    }

    public Long getTimeStamp() {
      return timeStamp;
    }

    public void setLocations(String[] locations) {
      this.locations = locations;
    }

    public String[] getLocations() {
      return locations;
    }

    public void setIsAllZeros(boolean allZeros) {
      isAllZeros = allZeros;
    }

    public boolean getIsAllZeros() {
      return isAllZeros;
    }

    public void setIsParity(boolean parity) {
      isParity = parity;
    }

    public boolean getIsParity(){
      return isParity;
    }

    public void setIsMissingBlock(boolean isMissingBlk){
      isMissingBlock = isMissingBlk;
    }

    public boolean getIsMissingBlock(){
      return isMissingBlock;
    }

  }

  static class BlockGroup {
    static enum Corruption_Type {
      ALL_ZEROS_WITH_ADDITIONAL_BLOCKS, MORE_VARIED_TIME_STAMPS;
    }

    private BlockGroup.Corruption_Type type;
    private List<Block> blocks;

    public BlockGroup(List<Block> blocks,
        BlockGroup.Corruption_Type type) {
      this.blocks = blocks;
      this.type = type;
    }

    public void setBlocks(List<Block> blocks) {
      this.blocks = blocks;
    }

    public List<Block> getBlocks() {
      return blocks;
    }
  }

  static class PossibleImpactedECFile {
    private String fileName;
    private String policyName;
    private int blockGroupSize;
    private List<BlockGroup> blockGroups;

    public PossibleImpactedECFile(String fileName, String ecPolicyName, int blockGroupSize,
        List<BlockGroup> blkGrps) {
      this.fileName = fileName;
      this.policyName = ecPolicyName;
      this.blockGroupSize = blockGroupSize;
      this.blockGroups = blkGrps;
    }

    public void setFileName(String fileName) {
      this.fileName = fileName;
    }

    public String getFileName() {
      return fileName;
    }

    public int getBlockGroupSize() {
      return blockGroupSize;
    }

    public void setBlockGroupSize(int blockGroupSize) {
      this.blockGroupSize = blockGroupSize;
    }

    public void setPolicyName(String policyName) {
      this.policyName = policyName;
    }

    public String getPolicyName() {
      return policyName;
    }

    public void setBlockGroups(List<BlockGroup> blockGroups) {
      this.blockGroups = blockGroups;
    }

    public List<BlockGroup> getBlockGroups() {
      return blockGroups;
    }
  }
}

