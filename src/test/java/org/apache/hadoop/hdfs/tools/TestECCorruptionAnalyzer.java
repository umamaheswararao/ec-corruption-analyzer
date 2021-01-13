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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.StripedFileTestUtilAccessor;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.MappingJsonFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;

/**
 * This class tests the parsing of DN block stats and building the EC analysis
 * report.
 */
public class TestECCorruptionAnalyzer {
  private Configuration conf;
  private MiniDFSCluster cluster;
  private DistributedFileSystem dfs;
  private final ErasureCodingPolicy ecPolicy =
      StripedFileTestUtil.getDefaultECPolicy();
  private int numDNs;
  private final int cellSize = ecPolicy.getCellSize();
  private final int dataBlocks = ecPolicy.getNumDataUnits();
  private final int parityBlocks = ecPolicy.getNumParityUnits();
  private final int blockSize = cellSize;
  private final int blockGroupSize = blockSize * dataBlocks;
  private final Path ecDir = new Path("/" + this.getClass().getSimpleName());

  private Path statsDirPath = new Path("/test/ecblockstats");
  private Path zeroBlocksPath =
      new Path(statsDirPath, ECCorruptFilesAnalyzer.ALL_ZEROS_BLOCKS_FOLDER);
  private Path timestampsPath =
      new Path(statsDirPath, ECCorruptFilesAnalyzer.BLOCK_TIME_STAMPS_FOLDER);

  protected Configuration createConfiguration() {
    return new HdfsConfiguration();
  }

  @Before
  public void setup() throws IOException {
    conf = createConfiguration();
    conf.set("dfs.block.local-path-access.user", UserGroupInformation.getCurrentUser().getUserName());
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    numDNs = dataBlocks + parityBlocks + 6;
    init(numDNs);
  }

  private void init(int numDNs) throws IOException {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
    cluster.waitActive();
    dfs = cluster.getFileSystem(0);

    dfs.enableErasureCodingPolicy(
        StripedFileTestUtil.getDefaultECPolicy().getName());
    dfs.mkdirs(ecDir);
    dfs.setErasureCodingPolicy(ecDir,
        StripedFileTestUtil.getDefaultECPolicy().getName());
    dfs.mkdirs(zeroBlocksPath);
    dfs.mkdirs(timestampsPath);
  }

  @After
  public void teardown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test(timeout = 30000)
  public void testECCorruptFileAnalyzerWithBlockGroups() throws Exception {
    List<Byte> busyNodes = new ArrayList<>();
    busyNodes.add((byte)6);
    //1. create EC file
    final Path ecFile = new Path(ecDir, "ECCorruptFileAnalyzer");
    int writeBytes = cellSize * dataBlocks;
    writeStripedFile(dfs, ecFile, writeBytes);
    generateStatsFiles(busyNodes, ecFile, 5 * 60 * 1000 + 1);
    List<String> files = analyzeECCorruption();
    Assert.assertEquals(1, files.size());
  }

  @Test(timeout = 300000)
  public void testECCorruptFileAnalyzerWithMultipleBlockGroups() throws Exception {
    List<Byte> corruptBlockIndices = new ArrayList<>();
    corruptBlockIndices.add((byte)1);
    corruptBlockIndices.add((byte)2);
    corruptBlockIndices.add((byte)6);

    //2 block group sizes
    int writeBytes = 2 * cellSize * dataBlocks;

    final Path ecRecoverableFile1 = new Path(ecDir, "ECCorruptFileAnalyzer-recoverable");
    writeECFile(ecRecoverableFile1, writeBytes);

    final Path ecRecoverableFile2 = new Path(ecDir, "ECCorruptFileAnalyzer-recoverable-1");
    writeECFile(ecRecoverableFile2, writeBytes);

    //Simulating to generate block statistics from all DNs for a given file blocks
    generateStatsFiles(corruptBlockIndices, ecRecoverableFile1, 5 * 60 * 1000 + 1);
    generateStatsFiles(corruptBlockIndices, ecRecoverableFile2, 5 * 60 * 1000 + 1);


    // >4 corrupt: making below file unrecoverable
    corruptBlockIndices.add((byte)8);
    final Path ecUnrecoverableFile1 = new Path(ecDir, "ECCorruptFileAnalyzer-unrecoverable");
    writeECFile(ecUnrecoverableFile1, writeBytes);

    //Simulating to generate block statistics from all DNs for a given file blocks
    generateStatsFiles(corruptBlockIndices, ecUnrecoverableFile1,  5 * 60 * 1000 + 1);

    List<String> corruptedFiles = analyzeECCorruption();
    Assert.assertEquals(3, corruptedFiles.size());
  }

  @Test(timeout = 30000)
  public void testECCorruptFileAnalyzerWithBlockGroupsWhenOnlyDatablockNumDNsHaveBlocks()
      throws Exception {
    cluster.shutdown();
    init(dataBlocks);
    List<Byte> busyNodes = new ArrayList<>();
    busyNodes.add((byte) 5);
    //1. create EC file
    final Path ecFile = new Path(ecDir, "ECCorruptFileAnalyzer");
    int writeBytes = cellSize * dataBlocks;
    writeECFile(ecFile, writeBytes);
    generateStatsFiles(busyNodes, ecFile, 5 * 60 * 1000 + 1);
    List<String> files = analyzeECCorruption();
    Assert.assertEquals(1, files.size());
  }

  private void writeECFile(Path ecFile, int writeBytes) throws IOException {
    try (FSDataOutputStream fos = dfs.create(ecFile)) {
      BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fos));
      for (int i = 0; i < writeBytes; i++) {
        br.write('c');
      }
      br.close();
    }
  }

  private void generateStatsFiles(List<Byte> busyDNIndexes, Path ecFile, int timeToIncr)
      throws IOException {
    DirectoryListing directoryListing =
        dfs.getClient().listPaths(ecFile.toString(), "".getBytes(), true);
    HdfsFileStatus[] partialListing = directoryListing.getPartialListing();
    LocatedBlocks locatedBlocks =
        ((HdfsLocatedFileStatus) partialListing[0]).getBlockLocations();
    for (int bg = 0; bg < locatedBlocks.getLocatedBlocks().size(); bg++) {
      LocatedBlock[] lbs = StripedBlockUtil.parseStripedBlockGroup(
          (LocatedStripedBlock) locatedBlocks.getLocatedBlocks().get(bg),
          cellSize, dataBlocks, parityBlocks);
      long timeNow = System.currentTimeMillis();
      for (int i = 0; i < dataBlocks + parityBlocks; i++) {
        if(lbs[i] == null) continue;
        java.nio.file.Path path = new File(
            cluster.getDataNode(lbs[i].getLocations()[0].getIpcPort())
                .getBlockLocalPathInfo(lbs[i].getBlock(),
                    lbs[i].getBlockToken()).getBlockPath()).toPath();
        FileTime lastModifiedTime = Files.getLastModifiedTime(path);
        Path timeStampsFile = new Path(timestampsPath,
            lbs[i].getLocations()[0].getIpAddr() + lbs[i].getLocations()[0]
                .getIpcPort() + ".blocktimestamps");
        try (FSDataOutputStream fos = dfs.exists(timeStampsFile) ?
            dfs.append(timeStampsFile) :
            dfs.create(timeStampsFile)) {
          BufferedWriter wr = new BufferedWriter(new OutputStreamWriter(fos));
          long time = timeNow;
          if (busyDNIndexes.contains((byte)i)) {
            // in Hadoop >3.3.0 versions the bug has been fixed. So all zero
            // blocks will not be reconstructed. This is to simulate the case and
            // it should have higher time stamp.
            time = time + timeToIncr;
          }
          wr.write(time + " " + path);
          wr.newLine();
          wr.close();
        }

        if (busyDNIndexes.contains((byte)i)) {
          // In >Hadoop 3.3.0 versions the bug has been fixed. So all zero
          // blocks will not be reconstructed. This is to simulate the case and
          // if it happens in older versions, the AllZero block should detected
          // and added into this file.
          Path allZeros = new Path(zeroBlocksPath,
              lbs[i].getLocations()[0].getIpAddr() + lbs[i].getLocations()[0]
                  .getIpcPort() + ".allzerosblks");
          try (FSDataOutputStream fos = dfs.exists(allZeros) ?
              dfs.append(allZeros) :
              dfs.create(allZeros)) {
            BufferedWriter wr = new BufferedWriter(new OutputStreamWriter(fos));
            wr.write(path.toString());
            wr.newLine();
            wr.close();
          }
        }
      }
    }
  }

  private List<String> analyzeECCorruption()
      throws Exception {
    List<Path> paths = new ArrayList<>();
    paths.add(new Path("/"));
    ECCorruptFilesAnalyzer analyzer = new ECCorruptFilesAnalyzer();
    Path out = new Path("/scanning");
    analyzer.analyze(statsDirPath, paths, out, conf);
    analyzer.stop();
    List<String> files = getImpactedFiles(new Path(out, "ConsolidatedResult"));
    return files;
  }

  private List<String> getImpactedFiles(Path out) throws Exception {
    FileSystem fs = out.getFileSystem(conf);
    List<String> files = new ArrayList<>();
    JsonFactory jf = new MappingJsonFactory();
    try (FSDataInputStream open = fs.open(out)) {
      BufferedReader reader = new BufferedReader(new InputStreamReader(open));
      JsonParser parser = jf.createJsonParser(reader);
      JsonToken current;
      current = parser.nextToken();
      if (current != JsonToken.START_OBJECT) {
        System.out.println("Quiting...root should be obj");
      }
      while (current != null && current != JsonToken.END_OBJECT) {
        JsonNode jn = parser.readValueAsTree();
        files.add(jn.get("fileName").getTextValue());
        current = parser.nextToken();
      }
    }
    return files;
  }

  private byte[] writeStripedFile(DistributedFileSystem fs, Path ecFile,
      int writeBytes) throws Exception {
    byte[] bytes = StripedFileTestUtil.generateBytes(writeBytes);
    DFSTestUtil.writeFile(fs, ecFile, new String(bytes));
    StripedFileTestUtil.waitBlockGroupsReported(fs, ecFile.toString());

    StripedFileTestUtilAccessor
        .checkData(fs, ecFile, writeBytes, new ArrayList<DatanodeInfo>(), null,
            blockGroupSize);
    return bytes;
  }
}
