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

import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URISyntaxException;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.StripedFileTestUtilAccessor;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;

/**
 * This class tests the decommissioning of datanode with striped blocks.
 */
public class TestECCorruptionAnalyzer {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestECCorruptionAnalyzer.class);

  // heartbeat interval in seconds
  private static final int HEARTBEAT_INTERVAL = 3;

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

  private FSNamesystem fsn;
  private Path statsDirPath = new Path("/test/ecblockstats");
  private Path zereBlocksPath =
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
    numDNs = dataBlocks + parityBlocks + 6;
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
    cluster.waitActive();
    dfs = cluster.getFileSystem(0);
    fsn = cluster.getNamesystem();

    dfs.enableErasureCodingPolicy(
        StripedFileTestUtil.getDefaultECPolicy().getName());
    dfs.mkdirs(ecDir);
    dfs.setErasureCodingPolicy(ecDir,
        StripedFileTestUtil.getDefaultECPolicy().getName());
    dfs.mkdirs(zereBlocksPath);
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
    byte busyDNIndex = 6;
    //1. create EC file
    final Path ecFile = new Path(ecDir, "ECCorruptFileAnalyzer");
    int writeBytes = cellSize * dataBlocks;
    writeStripedFile(dfs, ecFile, writeBytes);
    generateStatsFiles(busyDNIndex, ecFile);
    List<ECCorruptFilesAnalyzer.BlockGrpCorruptedBlocks> corruptedBlockGrps =
        analyzeECCorruption();
    Assert.assertEquals(1, corruptedBlockGrps.size());
  }

  private void generateStatsFiles(byte busyDNIndex, Path ecFile)
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
      for (int i = 0; i < dataBlocks + parityBlocks; i++) {
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
          long time = lastModifiedTime.toMillis();
          if (i == busyDNIndex) {
            // in Hadoop >3.3.0 versions the bug has been fixed. So all zero
            // blocks will not be reconstructed. This is to simulate the case and
            // it should have higher time stamp.
            time++;
          }
          wr.write(time + "=" + path);
          wr.newLine();
          wr.close();
        }

        if (i == busyDNIndex) {
          // In >Hadoop 3.3.0 versions the bug has been fixed. So all zero
          // blocks will not be reconstructed. This is to simulate the case and
          // if it happens in older versions, the AllZero block should detected
          // and added into this file.
          Path allZeros = new Path(zereBlocksPath,
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

  private List<ECCorruptFilesAnalyzer.BlockGrpCorruptedBlocks> analyzeECCorruption()
      throws IOException, URISyntaxException {
    ECCorruptFilesAnalyzer.initStats(statsDirPath, conf);
    Path[] paths = new Path[] {new Path("/")};
    ECCorruptFilesAnalyzer.Results res = new ECCorruptFilesAnalyzer.Results();
    ECCorruptFilesAnalyzer.processNamespace(paths, dfs, res);
    return res.getAllResults().entrySet().iterator().next().getValue();
  }

  /* Get DFSClient to the namenode */
  private static DFSClient getDfsClient(NameNode nn, Configuration conf)
      throws IOException {
    return new DFSClient(nn.getNameNodeAddress(), conf);
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

  private void cleanupFile(FileSystem fileSys, Path name) throws IOException {
    assertTrue(fileSys.exists(name));
    fileSys.delete(name, true);
    assertTrue(!fileSys.exists(name));
  }

  /*
   * Wait till node is fully decommissioned.
   */
  private void waitNodeState(DatanodeInfo node, AdminStates state) {
    boolean done = state == node.getAdminState();
    while (!done) {
      LOG.info(
          "Waiting for node " + node + " to change state to " + state + " current state: " + node
              .getAdminState());
      try {
        Thread.sleep(HEARTBEAT_INTERVAL * 500);
      } catch (InterruptedException e) {
        // nothing
      }
      done = state == node.getAdminState();
    }
    LOG.info("node " + node + " reached the state " + state);
  }
}
