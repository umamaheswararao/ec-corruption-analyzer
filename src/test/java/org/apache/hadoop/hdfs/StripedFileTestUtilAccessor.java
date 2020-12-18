package org.apache.hadoop.hdfs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

import java.io.IOException;
import java.util.ArrayList;

public class StripedFileTestUtilAccessor {

  public static void checkData(DistributedFileSystem fs, Path ecFile, int writeBytes, ArrayList<DatanodeInfo> datanodeInfos, Object o,
      int blockGroupSize) throws IOException {
     StripedFileTestUtil.checkData(fs, ecFile, writeBytes, datanodeInfos,null, blockGroupSize);
  }
}