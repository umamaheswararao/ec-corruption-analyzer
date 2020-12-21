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
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
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
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
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
import java.util.concurrent.TimeUnit;

/**
 * This class starts and runs EC Block Validation service.
 */
public class ECCorruptFilesAnalyzer {
  public static final Logger LOG =
      LoggerFactory.getLogger(ECCorruptFilesAnalyzer.class);
  private static final int SAFE_BLK_RENAME_PER_NODE_FILE_FLUSH_NUM = 15;

  private static ECBlockStatsProvider stats = new ECBlockStatsProvider();
  private static long EXPECTED_TIME_GAP_BETWEEN_BLOCKS = 2 * 1000;
  static String ALL_ZEROS_BLOCKS_FOLDER = "allzeroblocks";
  static String BLOCK_TIME_STAMPS_FOLDER = "blocktimestamps";
  private static final  HdfsConfiguration conf = new HdfsConfiguration();

  public static void initStats(Path ecBlockStatsPath, Configuration conf)
      throws IOException, URISyntaxException {
    stats.init(ecBlockStatsPath, conf);
  }

  /**
   * Main method to start validating service service.
   */
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println(
          "Not sufficient arguments provided. Expected parameters are \n" +
              " 1. statsPath - where scripts/EC_Blks_TimeStamp_And_AllZeroBlks_Finder.sh generated outputs,\n" +
              " 2. list of target Paths to scan for EC files. \n" +
              " 3. Output directory path.");
      return;
    }
    Path ecBlockStatsPath = new Path(args[0]);
    String split[] = args[1].split(",");
    Path targetPaths[] = new Path[split.length];
    for (int i = 0; i < split.length; i++) {
      targetPaths[i] = new Path(split[i]);
    }

    Path outPath = args.length>2 ? new Path(args[2]) : null;

    //For now just use balancer key tab
    secureLogin(conf);
    DistributedFileSystem dfs = null;
    ResultsProcessor results = new ResultsProcessor(outPath);
    try {
      dfs = new DistributedFileSystem();
      dfs.initialize(FileSystem.getDefaultUri(conf), conf);
      initStats(ecBlockStatsPath, conf);
      results.start();
      processNamespace(targetPaths, dfs, results);
      System.out.println(results);
    } finally {
      results.stopProcessorGracefully();
      if (dfs != null) {
        dfs.close();
      }
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

  public static void processNamespace(Path[] targetPaths,
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
      final int cellSize = ecPolicy.getCellSize();
      final int dataBlkNum = ecPolicy.getNumDataUnits();
      final int parityBlkNum = ecPolicy.getNumParityUnits();
      List<BlockGrpCorruptedBlocks> bgCorruptBlks = new ArrayList<>();
      // Scan all block groups in this file
      for (LocatedBlock firstBlock : locatedBlocks.getLocatedBlocks()) {
        LocatedBlock[] blocks = StripedBlockUtil
            .parseStripedBlockGroup((LocatedStripedBlock) firstBlock, cellSize,
                dataBlkNum, parityBlkNum);
        //check if any internal block has allZeros
        List<LocatedBlock> allZeroBlks = new ArrayList<>();
        PriorityQueue<BlockWithStats> pq =
            new PriorityQueue<>(new Comparator<BlockWithStats>() {
              @Override
              public int compare(BlockWithStats o1, BlockWithStats o2) {
                return o1.getTime().compareTo(o2.getTime());
              }
            });

        for (LocatedBlock block : blocks) {
          String[] locs=Arrays.stream(block.getLocations()).map(datanodeInfo -> {
            return datanodeInfo.getIpAddr()+":"+datanodeInfo.getIpcPort();
          }).toArray(String[]::new);

          if (stats.allZeroBlockIds.contains(block.getBlock()) && isParityBlock(
              block.getBlock(), dataBlkNum)) { //Currently
            allZeroBlks.add(block);
            pq.offer(new BlockWithStats(block.getBlock(),
                new Stats(stats.getModifiedTime(block.getBlock()),
                    stats.getBlockWithPath(block.getBlock()), true,
                    locs)));
          } else {
            //TODO:// if no blocks in DNs, then modified time is Long.MAX and path is ""
            pq.offer(new BlockWithStats(block.getBlock(),
                new Stats(stats.getModifiedTime(block.getBlock()),
                    stats.getBlockWithPath(block.getBlock()), false,
                    locs)));
          }
        }
        if (allZeroBlks.size() > 0) { // Found all zero blocks
          //Find first created zero block
          //We are relying on Inode modification time
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
            BlockWithStats peek = pq.peek();
            if (peek.getTime() < firstZeroBlkTime) {
              pq.remove();
            } else {
              break;
            }
          }
          bgCorruptBlks.add(
              new BlockGrpCorruptedBlocks(Lists.newArrayList(pq.iterator()),
                  BlockGrpCorruptedBlocks.Corruption_Type.ALL_ZEROS_WITH_ADDITIONAL_BLOCKS,
                  parityBlkNum));
          /*results.addToResult(fullPath,
              new BlockGrpCorruptedBlocks(Lists.newArrayList(pq.iterator()),
                  BlockGrpCorruptedBlocks.Corruption_Type.ALL_ZEROS_WITH_ADDITIONAL_BLOCKS));*/
        } else { //No all Zero blocks, but lets check the time variation between
          // blocks find the time stamps which are going through PQ and check
          // the time diffs from first created block...Collect all block which
          // are more than 2s away from first created block
          List<BlockWithStats> possibleCorruptions = new ArrayList<>();
          while (!pq.isEmpty()) {
            BlockWithStats nextCreatedBlk = pq.remove();
            if (Math.abs(
                nextCreatedBlk.stats.time - inodeModificationTime) > EXPECTED_TIME_GAP_BETWEEN_BLOCKS) {
              possibleCorruptions.add(nextCreatedBlk);
            }
          }

          if (possibleCorruptions.size() > 0) {
            bgCorruptBlks.add(new BlockGrpCorruptedBlocks(possibleCorruptions,
                BlockGrpCorruptedBlocks.Corruption_Type.MORE_VARIED_TIME_STAMPS,
                parityBlkNum));
          }
        }
      }

      if(bgCorruptBlks.size()>0){
        results.addToResult(fullPath, bgCorruptBlks);
      }
    }
  }

  private static boolean isParityBlock(ExtendedBlock block, int i) {
    return StripedBlockUtil.getBlockIndex(block.getLocalBlock()) >= i;
  }

  /**
   *
   */
  static class ECBlockStatsProvider {
    FileSystem fs;
    // <ECBlockStatsDir>/allzeroblocks/dnhost.txt
    //dnhost
    // <ECBlockStatsDir>/blocktimestamps/
    private List<ExtendedBlock> allZeroBlockIds = new ArrayList<>();
    private Map<ExtendedBlock, Stats> blockVsModifiedTime = new HashMap<>();

    public void init(Path statsPath, Configuration conf)
        throws IOException, URISyntaxException {
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

      return new ExtendedBlock(bpID, Block.filename2id(blockFileStr));
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
      long miilisTime = Long.valueOf(split[1]);
      long timeStampInMillis = secsInMillis + miilisTime;
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

  static class BlockWithStats {
    private ExtendedBlock block;
    private Stats stats;

    public BlockWithStats(ExtendedBlock block, Stats stats) {
      this.block = block;
      this.stats = stats;
    }

    public Long getTime() {
      return stats.time;
    }

    public ExtendedBlock getBlock() {
      return this.block;
    }

    public boolean isAllZerosBlock() {
      return stats.isAllZeros;
    }

    @Override
    public String toString() {
      return "BlockWithStats{" + "block=" + block + ", stats=" + stats + '}';
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
      return "Stats{" + "time=" + time + ", path='" + path + '\'' + ", isAllZeros=" + isAllZeros + " lications: " + Arrays
          .toString(locations) + '}';
    }
  }

  public static class ResultsProcessor extends Thread {
    private final Path consolidatedResultPath;
    private final Path unrecoverableBlkGrpResultPath;
    private BufferedWriter bwForConsolidatedResultPath = null;
    private BufferedWriter bwForunrecoverableBlkGrpResultPath = null;
    private Path outPutPath;
    private Path safeBlksToRenamePath;
    private FileSystem fs;

    public ResultsProcessor(Path outPutPath) throws IOException {
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
        bwForunrecoverableBlkGrpResultPath = new BufferedWriter(
            new OutputStreamWriter(
                fs.create(this.unrecoverableBlkGrpResultPath)));
      }
    }

    private Map<String, List<BlockGrpCorruptedBlocks>> results =
        new HashMap<>();
    private static volatile boolean running = false;

    private Queue<CorruptFileBlockGroups> queue = new LinkedList<>();
    private Map<String, List<String>> safeBlocksToRename = new HashMap<>();

    public ResultsProcessor() throws IOException {
      this(null);
    }

    public Map<String, List<BlockGrpCorruptedBlocks>> getAllResults() {
      return this.results;
    }

    public void addToResult(String file, List<BlockGrpCorruptedBlocks> bgCorruptBlks) {
      queue.offer(new CorruptFileBlockGroups(file, bgCorruptBlks));
    }

    @Override
    public void run() {
      running = true;
      while(running){
        CorruptFileBlockGroups fileBlkGrps = queue.poll();
        if(fileBlkGrps!=null) {
          if(this.outPutPath == null){
            // This is just in memory and testing.
            results.put(fileBlkGrps.getFileName(), fileBlkGrps.corruptBlockGroups);
            continue;
          }
          List<BlockGrpCorruptedBlocks> blockGrpCorruptedBlocks =
              fileBlkGrps.getBlockGrpCorruptedBlocks();
          for(int i=0;i< blockGrpCorruptedBlocks.size(); i++){
            BlockGrpCorruptedBlocks blkGrp =
                blockGrpCorruptedBlocks.get(i);
            if (blkGrp.getBlocks().size() <= blkGrp.parity) {
              System.out.println(
                  "Found recoverable block group in file:" + fileBlkGrps
                      .getFileName() + " impacted blks : " + blkGrp
                      .getBlocks());
              for (BlockWithStats blk : blkGrp.getBlocks()) {
                //In EC, do we get more than one locations returned?
                List<String> paths = safeBlocksToRename
                    .getOrDefault(blk.stats.locations[0], new ArrayList<>());
                paths.add(blk.stats.path);
                safeBlocksToRename.put(blk.stats.locations[0], paths);
              }
            }else{
              //unrecoverable block groups detected.
              try {
                bwForunrecoverableBlkGrpResultPath.write(blkGrp.toString());
                bwForunrecoverableBlkGrpResultPath.newLine();
                bwForunrecoverableBlkGrpResultPath.flush();
              } catch (IOException e) {
                LOG.error("Unable to write unrecoverable block groups details", e);
              }
            }

            //consolidated
            try {
              bwForConsolidatedResultPath.write(blkGrp.toString());
              bwForConsolidatedResultPath.newLine();
              bwForConsolidatedResultPath.flush();
            } catch (IOException e) {
              LOG.error("Unable to write consolidated impacted block groups details", e);
            }

          }

          //check if we can flush to files
          if(safeBlocksToRename.size()>0){
            writeSafeToRenameBlockPaths(safeBlocksToRename,
                SAFE_BLK_RENAME_PER_NODE_FILE_FLUSH_NUM);
          }


        }else{
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            //
          }
        }
      }
    }

    private synchronized void writeSafeToRenameBlockPaths(
        Map<String, List<String>> safeBlocksToRename, int flushNum) {
      Iterator<Map.Entry<String, List<String>>> iter =
          safeBlocksToRename.entrySet().iterator();
      while(iter.hasNext()){
        Map.Entry<String, List<String>> next = iter.next();
        List<String> paths = next.getValue();
        if(paths.size()>flushNum){ //make it configurable
          //Let's flush
          if(fs!=null && safeBlksToRenamePath !=null){
            Path filePath = new Path(
                safeBlksToRenamePath, next.getKey().replace(":","@"));
            try {
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

      if(bwForunrecoverableBlkGrpResultPath!=null){
        bwForunrecoverableBlkGrpResultPath.close();
      }

      this.interrupt();
    }

    @Override
    public String toString() {
      return "Results{" + "results=" + queue + '}';
    }
  }

  static class CorruptFileBlockGroups{
     private String file;
     private List<BlockGrpCorruptedBlocks> corruptBlockGroups;

     public CorruptFileBlockGroups(String file, List<BlockGrpCorruptedBlocks> corruptBlockGroups){
       this.file = file;
       this.corruptBlockGroups = corruptBlockGroups;
     }

     public String getFileName(){
       return this.file;
     }

    public List<BlockGrpCorruptedBlocks> getBlockGrpCorruptedBlocks(){
      return this.corruptBlockGroups;
    }
  }

  static class BlockGrpCorruptedBlocks {
    private final int parity;

    static enum Corruption_Type {
      ALL_ZEROS_WITH_ADDITIONAL_BLOCKS, MORE_VARIED_TIME_STAMPS;
    }

    private List<BlockWithStats> blkWithTimeStamp;
    private Corruption_Type type;

    public BlockGrpCorruptedBlocks(List<BlockWithStats> blkWithTimeStamp,
        Corruption_Type type, int parity) {
      this.blkWithTimeStamp = blkWithTimeStamp;
      this.type = type;
      this.parity = parity;
    }

    public List<BlockWithStats> getBlocks() {
      return this.blkWithTimeStamp;
    }

    @Override
    public String toString() {
      return "BlockGrpCorruptedBlocks{" + "blkWithTimeStamp=" + blkWithTimeStamp + ", type=" + type + '}';
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
}

