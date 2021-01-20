import org.apache.commons.cli.Options;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.Socket;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class LauncherGetFileStatusMulti {
    /*public static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(LauncherGetFileStatusMulti.class.getName());*/

    public static final Logger LOG = Logger
            .getLogger(LauncherGetFileStatusMulti.class.getName());
    public static final PathFilter HIDDEN_FILES_PATH_FILTER = new PathFilter() {
        public boolean accept(Path p) {
            String name = p.getName();
            return !name.startsWith("_") && !name.startsWith(".");
        }
    };
    static CopyOnWriteArrayList<Operation> totalResponseTimes = null;
    static FileSystem fileSystem = null;
    static Long nnResponseTime ;
    static Long dnResponseTime;
    static String blockids;
    static boolean storeMetrics = false;
    static boolean waitForSocketClosure = false;
    static boolean getBlockLocations = false;
    private static ExecutorService executor = null;
    private static boolean get = false;
    private static boolean getmd5 = false;
    private static HashMap<String,LongSummaryStatistics> longSummaryStatisticsHashMap = null;
    public static void main(String[] args) throws IOException, InterruptedException {
        totalResponseTimes = new CopyOnWriteArrayList<>();
        longSummaryStatisticsHashMap = new HashMap<>();
        //####################LOG FILE INIT##############//
        Date date = new Date();
        SimpleDateFormat DateFor = new SimpleDateFormat("MM-dd-yyyy-HH-mm-ss-SS");
        System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS,%1$tL %4$-6s %2$s %5$s%6$s%n");
        String stringDate = DateFor.format(date);
        String logfileName = "./logfile-" + stringDate + ".log";
        FileHandler fileHandler = new FileHandler(logfileName);
        SimpleFormatter simple = new SimpleFormatter();
        fileHandler.setFormatter(simple);
        LOG.addHandler(fileHandler);
        //TBO LOG.info("Starting Cloudera Test Program:");
        //####################EVALUATE COMMAND LINE ARGUMENTS##############//
        CommandLine cmd = evaluateCommandLineOptions(args);
        String confPath = cmd.getOptionValue("conf-path","/etc/hadoop/conf");
        //TBO LOG.info("conf-path:" + confPath);
        String hdfsFilePath = cmd.getOptionValue("hdfs-path");
        //TBO LOG.info("hdfs-path:" + hdfsFilePath);
        blockids = cmd.getOptionValue("blockids");
        LOG.info("blockids:" + blockids);
        boolean useMultiClient = cmd.hasOption("use-multiple-clients");
        //TBO LOG.info("use-multiple-clients:" + useMultiClient);
        boolean withFilter = cmd.hasOption("ignore-hidden-files");
        //TBO LOG.info("ignore-hidden-files:" + withFilter);
        String[] noOfThreadsSequence = null;
        if(!cmd.hasOption("no-of-threads"))
        {
            noOfThreadsSequence = new String[]{"1"};
        }else{
            noOfThreadsSequence = cmd.getOptionValues("no-of-threads");
        }
        for(String noOfThreads:noOfThreadsSequence){
            //TBO LOG.info("no-of-threads:" + noOfThreads);
        }

        nnResponseTime = Long.parseLong(cmd.getOptionValue("nn-response-limit", "100"))*1000000;
        //TBO LOG.info("nn-response-limit:" + nnResponseTime);
        dnResponseTime = Long.parseLong(cmd.getOptionValue("dn-response-limit", "1000"))*1000000;
        //TBO LOG.info("dn-response-limit:" + dnResponseTime);
        storeMetrics = cmd.hasOption("store-metrics");
        //TBO LOG.info("store-metrics:" + storeMetrics);
        waitForSocketClosure = cmd.hasOption("wait-for-socket-closure");
        //TBO LOG.info("wait-for-socket-closure:" + waitForSocketClosure);


        String[] operations = cmd.getOptionValues("operations");
        if (operations == null || operations.length == 0) {
            operations = new String[]{cmd.getOptionValue("operations", "listStatus")};
        }

        List<String> operationsList = Arrays.asList(operations);
        getBlockLocations = operationsList.contains("getBlockLocations");
        get = operationsList.contains("get");
        getmd5 = operationsList.contains("getmd5");


        String coreSite = confPath + "/core-site.xml";
        String hdfsSite = confPath + "/hdfs-site.xml";

        //####################INIT FILESYSTEM and KICK START THE EXECUTION##############//
        for(String noOfThreads:noOfThreadsSequence){
            //TBO LOG.info("ATTEMPT START with noOfThreads:"+noOfThreads);
            executor = Executors.newFixedThreadPool(Integer.parseInt(noOfThreads));
            FileSystem fileSystemLocal = getFileSystem(coreSite, hdfsSite, useMultiClient);
            performFileOperations(fileSystemLocal, new Path(hdfsFilePath), withFilter);
            executor.shutdown();
            try {
                if (!executor.awaitTermination(20, TimeUnit.MINUTES)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
            }

            //####################MONITOR CLIENT SOCKET STATUS AT THE END##############//
            List<Socket> socketsList= new ArrayList<>();
            //TBO LOG.info("Before All Sockets Closed");
            String localIPAddress = "";
            for (Socket socket : CustomizedSocketFactory.SOCKETLIST) {
                if (socket != null) {
                    if(localIPAddress.isEmpty()){
                        localIPAddress = socket.getLocalAddress().getHostAddress();
                    }
                    socketsList.add(socket);
                    //TBO LOG.info("Socket: " + socket.toString() + " :Closed-" + socket.isClosed() + ",Connected-" + socket.isConnected() + "," +
                    //TBO "Bound-" + socket.isBound() + ",InputShutdown-" + socket.isInputShutdown() + ",OutputShutdown-" + socket.isOutputShutdown());
                }
            }

            //TBO LOG.info("Sockets List:"+socketsList);
            if(waitForSocketClosure){
                LinkedHashSet<Socket> socketsClosedLHS = new LinkedHashSet<>();
                while (socketsClosedLHS.size() != CustomizedSocketFactory.SOCKETLIST.size()) {
                    for (Socket socket : CustomizedSocketFactory.SOCKETLIST) {
                        if (socket != null) {
                            if (socket.isClosed()) {
                                socketsClosedLHS.add(socket);
                            }
                        }
                    }
                    LOG.info("Sleeping for 10 seconds (Waiting for all sockets to get closed)");
                    Thread.sleep(10000);
                }
                LOG.info("After All Sockets Closed");
                for (Socket socket : CustomizedSocketFactory.SOCKETLIST) {
                    if (socket != null) {
                        LOG.info("Socket: " + socket.toString() + " :Closed-" + socket.isClosed() + ",Connected-" + socket.isConnected() + "," +
                                "Bound-" + socket.isBound() + ",InputShutdown-" + socket.isInputShutdown() + ",OutputShutdown-" + socket.isOutputShutdown());
                    }
                }
                collectNetstatOutput(socketsList);
            }

            if(storeMetrics){
                summarizeMetrics(noOfThreads);
            }
            //TBO LOG.info("ATTEMPT END with noOfThreads:"+noOfThreads);
        }

        if(storeMetrics){
            LOG.info("PRINTING FINAL BRIEF SUMMARY");
            LOG.info("############################");
            for(String key:longSummaryStatisticsHashMap.keySet())
            {
                LOG.info("No-of-threads and operation:"+ key+ " ,Summary:"+longSummaryStatisticsHashMap.get(key));
            }
            LOG.info("############################");
        }
        System.out.println("Your log file is placed at:" + logfileName);
    }

    private static long getResponseTime(Operation.OPERATIONS operation)
    {
        switch(operation){
            case GET:
                return dnResponseTime;
            case GETMD5:
                return dnResponseTime;
            default:
                return nnResponseTime;
        }
    }
    private static void summarizeMetrics(String noOfThreads) {
        HashMap<Operation.OPERATIONS,List<Operation>> operationsListHashMap = new HashMap<>();
        for(Operation.OPERATIONS operationType: Operation.OPERATIONS.values()){
            operationsListHashMap.put(operationType,new ArrayList<Operation>());
        }
        for(Operation operation:totalResponseTimes){
            operationsListHashMap.get(operation.name).add(operation);
        }
        for(Operation.OPERATIONS operationType: Operation.OPERATIONS.values()){
            LongStream longStreamSorted = operationsListHashMap.get(operationType).stream().map(Operation::getDuration).collect(Collectors.toList()).stream().mapToLong(Long::longValue).sorted();
            LongSummaryStatistics longSummaryStatistics = longStreamSorted.summaryStatistics();
            longSummaryStatisticsHashMap.put(noOfThreads+"#"+operationType,longSummaryStatistics);
            if(longSummaryStatistics.getCount() == 0)
            {
                continue;
            }
            LOG.info("Operation type:"+operationType);
            LOG.info("Summary:"+longSummaryStatistics);
            long maxTimeTaken = longSummaryStatistics.getMax();
            long minTimeTaken = longSummaryStatistics.getMin();
            int noOfSamples = 10;
            Long samplingTime = (maxTimeTaken-minTimeTaken)/noOfSamples;
            Long[] sampleCheckPoints = new Long[noOfSamples];
            for(int i=0;i<noOfSamples;i++){
                sampleCheckPoints[i] = minTimeTaken+(samplingTime*i);
            }
            int s =0;
            for(;s<noOfSamples-1;s++){
                int finalS = s;
                longStreamSorted = operationsListHashMap.get(operationType).stream().map(Operation::getDuration).collect(Collectors.toList()).stream().mapToLong(Long::longValue).sorted();
                LOG.info("No.of samples between "+sampleCheckPoints[s]+"(inclusive) and "+sampleCheckPoints[s+1]+"(exclusive):"+
                        longStreamSorted.filter(var -> var >= sampleCheckPoints[finalS] && var < sampleCheckPoints[finalS +1]).count());
            }
            longStreamSorted = operationsListHashMap.get(operationType).stream().map(Operation::getDuration).collect(Collectors.toList()).stream().mapToLong(Long::longValue).sorted();
            int finalS1 = s;
            LOG.info("No.of samples between "+sampleCheckPoints[s-1]+"(inclusive) and "+maxTimeTaken+"(inclusive):"+
                    longStreamSorted.filter(var -> var >= sampleCheckPoints[finalS1-1] && var == maxTimeTaken).count());

            longStreamSorted = operationsListHashMap.get(operationType).stream().map(Operation::getDuration).collect(Collectors.toList()).stream().mapToLong(Long::longValue).sorted();
            LOG.info("No.of samples crossing given threshold "+getResponseTime(operationType)+":"+
                    longStreamSorted.filter(var -> var > getResponseTime(operationType)).count());
        }
    }

    public static CommandLine evaluateCommandLineOptions(String[] args) {
        Options options = new Options();

        Option confPath = new Option("c", "conf-path", true, "(optional)hadoop conf path(default: /etc/hadoop/conf)");
        confPath.setOptionalArg(true);
        options.addOption(confPath);

        Option hdfsPath = new Option("f", "hdfs-path", true, "hdfs directory path");
        hdfsPath.setRequired(true);
        hdfsPath.setOptionalArg(false);
        options.addOption(hdfsPath);

        Option blockIds = new Option("b", "blockids", true, "comma separated EC blockids");
        blockIds.setRequired(true);
        blockIds.setOptionalArg(false);
        options.addOption(blockIds);

        Option ignoreHiddenFiles = new Option("i", "ignore-hidden-files", false, "(optional-default:false)Ignores files starting with '_' and '.'");
        ignoreHiddenFiles.setOptionalArg(true);
        options.addOption(ignoreHiddenFiles);

        Option useMultiClient = new Option("m", "use-multiple-clients", false, "(optional- default:true)Use Multiple Client sessions");
        useMultiClient.setOptionalArg(true);
        options.addOption(useMultiClient);

        Option noOfThreadsPerPool = new Option("n", "no-of-threads", true, "(optional - default:1)No.of Threads to run in parallel");
        noOfThreadsPerPool.setValueSeparator(',');
        noOfThreadsPerPool.setArgs(Option.UNLIMITED_VALUES);
        noOfThreadsPerPool.setOptionalArg(false);
        options.addOption(noOfThreadsPerPool);

        Option operations = new Option("o", "operations", true, "(optional - default:listStatus) Operations to perform:getBlockLocations,get,getmd5");
        operations.setOptionalArg(true);
        operations.setValueSeparator(',');
        options.addOption(operations);

        Option nnResponseLimit = new Option("r", "nn-response-limit", true, "(optional - default:100ms)Expected NameNode Response Turnaround Time(millis)");
        nnResponseLimit.setOptionalArg(true);
        options.addOption(nnResponseLimit);

        Option dnResponseLimit = new Option("d", "dn-response-limit", true, "(optional - default:1000ms)Expected DataNode Response Turnaround Time(millis)");
        dnResponseLimit.setOptionalArg(true);
        options.addOption(dnResponseLimit);

        Option storeMetrics = new Option("s", "store-metrics", false, "(optional- default:true)Store Metrics in a DataStructure For Advanced Processing");
        storeMetrics.setOptionalArg(true);
        options.addOption(storeMetrics);

        Option waitForSocketClosure = new Option("w", "wait-for-socket-closure", false, "(optional- default:false)Wait for all sockets to get closed");
        waitForSocketClosure.setOptionalArg(true);
        options.addOption(waitForSocketClosure);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            LOG.severe(e.getMessage());
            formatter.printHelp("Cloudera HDFSTest Tool:", options);
            System.exit(1);
        }
        return cmd;
    }

    public static void getListStatus(FileSystem fileSystem, Path path) throws IOException {
        fileSystem.listStatus(path, HIDDEN_FILES_PATH_FILTER);
    }

    public static void collectNetstatOutput(List<Socket> socketsList){
        try {

            // -- Linux --

            // Run a shell command
            // Process process = Runtime.getRuntime().exec("ls /home/mkyong/");

            // Run a shell script
            // Process process = Runtime.getRuntime().exec("path/to/hello.sh");

            // -- Windows --

            // Run a command
            //Process process = Runtime.getRuntime().exec("cmd /c dir C:\\Users\\mkyong");

            //Run a bat file
            String cmdToRun = "/usr/bin/netstat -plant";
            //TBO LOG.info("Command to run:"+cmdToRun);
            Process process = Runtime.getRuntime().exec(cmdToRun);

            StringBuilder output = new StringBuilder();

            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream()));

            String line;
            while ((line = reader.readLine()) != null) {
                String finalLine = line;
                if (socketsList.stream().filter(socket -> finalLine.contains(":"+socket.getLocalPort())).count() != 0) {
                    output.append(line + "\n");
                }
            }

            int exitVal = process.waitFor();
            if (exitVal == 0) {
                //TBO LOG.info("Success!");
                //TBO LOG.info(String.valueOf("\n"+output));
            } else {
                //abnormal...
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
    public static void performFileOperations(FileSystem fileSystem, Path path, boolean withFilter) {
        FileStatus fileStatus = null;
        try {
            fileStatus = fileSystem.getFileStatus(path);

            if (fileStatus != null) {
                if (fileStatus.isFile()) {
                    executor.submit(new GetFileStatus(path, fileSystem));
                    if (get) {
                        executor.submit(new DownloadFile(path, fileSystem));
                    }else if(getmd5) {
                        executor.submit(new DownloadFileWithMD5(path, fileSystem));
                    }
                } else if (fileStatus.isDirectory()) {
                    FileStatus[] fileStatuses = null;
                    if (withFilter) {
                        fileStatuses = fileSystem.listStatus(path, HIDDEN_FILES_PATH_FILTER);
                    } else {
                        fileStatuses = fileSystem.listStatus(path);
                    }

                    for (FileStatus fs : fileStatuses) {
                        performFileOperations(fileSystem, fs.getPath(), withFilter);
                    }
                }
            }
        } catch (IOException e) {
            LOG.severe(e.getMessage());
        }
    }

    public static FileSystem getFileSystem(String coreSite, String hdfsSite, boolean useMultiClient) {
        FileSystem filesystemLocal = null;
        if (!useMultiClient) {
            if (fileSystem == null) {
                fileSystem = LauncherGetFileStatusMulti.configureFileSystem(coreSite, hdfsSite);
                filesystemLocal = fileSystem;
            } else
                return fileSystem;
        } else {
            filesystemLocal = LauncherGetFileStatusMulti.configureFileSystem(coreSite, hdfsSite);
        }
        return filesystemLocal;
    }


    public static FileSystem configureFileSystem(String coreSitePath, String hdfsSitePath) {
        FileSystem fileSystem = null;
        try {
            Configuration conf = new Configuration();
            conf.setBoolean("dfs.support.append", true);
            conf.set("hadoop.rpc.socket.factory.class." + ClientProtocol.class.getSimpleName(), CustomizedSocketFactory.class.getName());
            Path coreSite = new Path(coreSitePath);
            Path hdfsSite = new Path(hdfsSitePath);
            conf.addResource(coreSite);
            conf.addResource(hdfsSite);
            fileSystem = FileSystem.get(conf);
        } catch (IOException ex) {
            LOG.severe("Failed loading FileSystem with given configuration " + coreSitePath + " and " + hdfsSitePath);
        }
        return fileSystem;
    }

    public static void downloadFiles(FileSystem fileSystem, Path path, ExecutorService executorService, boolean withFilter) {
        FileStatus fileStatus = null;
        try {
            fileStatus = fileSystem.getFileStatus(path);

            if (fileStatus != null) {
                if (fileStatus.isFile()) {
                    executor.submit(new DownloadFile(path, fileSystem));
                } else if (fileStatus.isDirectory()) {
                    FileStatus[] fileStatuses = null;
                    if (withFilter) {
                        fileStatuses = fileSystem.listStatus(path, HIDDEN_FILES_PATH_FILTER);
                    } else {
                        fileStatuses = fileSystem.listStatus(path);
                    }
                    for (FileStatus fs : fileStatuses) {
                        downloadFiles(fileSystem, fs.getPath(), executorService, withFilter);
                    }
                }
            }
        } catch (IOException e) {
            LOG.severe(e.getMessage());
        }
    }
}

class Operation {
    OPERATIONS name;
    Long time;
    Long duration;
    Operation(OPERATIONS name, Long time, Long duration) {
        this.name = name;
        this.time = time;
        this.duration = duration;
    }

    public OPERATIONS getName() {
        return name;
    }

    public void setName(OPERATIONS name) {
        this.name = name;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public Long getDuration() {
        return duration;
    }

    public void setDuration(Long duration) {
        this.duration = duration;
    }

    enum OPERATIONS {
        GETFILEINFO,GETBLOCKLOCATIONS,GET,GETMD5;
    }
}

class GetFileStatus implements Runnable {
    Path hdfsFilePath = null;
    FileSystem fileSystemLocal = null;

    GetFileStatus(Path hdfsFilePath, FileSystem fileSystem) {
        this.hdfsFilePath = hdfsFilePath;
        this.fileSystemLocal = fileSystem;
    }

    public void run() {
        try {
            DistributedFileSystem dfs = (DistributedFileSystem) fileSystemLocal;
            long start = System.nanoTime();
            HdfsFileStatus hdfsFileStatus = dfs.getClient().getFileInfo(hdfsFilePath.toUri().getPath());
            long end = System.nanoTime();
            long diffGetFileInfo = end - start;
            if(LauncherGetFileStatusMulti.storeMetrics) {
                LauncherGetFileStatusMulti.totalResponseTimes.add(new Operation(Operation.OPERATIONS.GETFILEINFO, System.nanoTime(), diffGetFileInfo));
            }
            String flagGetFileInfo = "";
            if (diffGetFileInfo > LauncherGetFileStatusMulti.nnResponseTime) {
                flagGetFileInfo = "HIGH";
            }
            LauncherGetFileStatusMulti.LOG.info("getFileInfo took " + diffGetFileInfo + " ns ;" + flagGetFileInfo + " Length of :" + hdfsFilePath.toString() + " Status:" + hdfsFileStatus.getLen());
            if (LauncherGetFileStatusMulti.getBlockLocations) {
                String fileName = hdfsFilePath.toString().replaceFirst(hdfsFilePath.toUri().getScheme() + "://", "");
                fileName = fileName.substring(fileName.indexOf("/"));
                start = System.nanoTime();
                BlockLocation[] blockLocations = dfs.getClient().getBlockLocations(fileName, 0L, hdfsFileStatus.getLen());
                end = System.nanoTime();
                long diffGetBlockLocations = end - start;
                if(LauncherGetFileStatusMulti.storeMetrics){
                    LauncherGetFileStatusMulti.totalResponseTimes.add(new Operation(Operation.OPERATIONS.GETBLOCKLOCATIONS, System.nanoTime(), diffGetBlockLocations));
                }
                String flagGetBlockLocations = "";
                if (diffGetBlockLocations > LauncherGetFileStatusMulti.nnResponseTime) {
                    flagGetBlockLocations = "HIGH";
                }
                LauncherGetFileStatusMulti.LOG.info("getBlockLocations took " + diffGetBlockLocations + " ns ;" + flagGetBlockLocations + " " + hdfsFilePath.toString());
                for (BlockLocation blockLocation : blockLocations) {
                    LauncherGetFileStatusMulti.LOG.info("getBlockLocations of " + hdfsFilePath.toString() + " :" + blockLocation.toString());
                }
            }
        } catch (Exception ex) {
            LauncherGetFileStatusMulti.LOG.severe(ex.getMessage());
            ex.printStackTrace();
        }
    }
}

class DownloadFile implements Runnable {
    Path hdfsFilePath = null;
    FileSystem fileSystemLocal = null;

    DownloadFile(Path hdfsFilePath, FileSystem fileSystem) {
        this.hdfsFilePath = hdfsFilePath;
        this.fileSystemLocal = fileSystem;
    }

    public void run() {
        try {
            DistributedFileSystem dfs = (DistributedFileSystem) fileSystemLocal;
            long start = System.nanoTime();
            BufferedReader fBufRdr = null;
            try {
                fBufRdr = new BufferedReader(new InputStreamReader(dfs.open(hdfsFilePath)));
                String line = fBufRdr.readLine();
                while (line != null) {
                    line = fBufRdr.readLine();
                }
                fBufRdr.close();
            } finally {
                if (fBufRdr != null) fBufRdr.close();
            }
            long end = System.nanoTime();
            long diffGet = end - start;
            if(LauncherGetFileStatusMulti.storeMetrics) {
                LauncherGetFileStatusMulti.totalResponseTimes.add(new Operation(Operation.OPERATIONS.GET, System.nanoTime(), diffGet));
            }
            String flagDownloadFile = "";
            if (diffGet > LauncherGetFileStatusMulti.dnResponseTime) {
                flagDownloadFile = "HIGH";
            }
            LauncherGetFileStatusMulti.LOG.info("get took " + diffGet + " ns ;" + flagDownloadFile + " Length of :" + hdfsFilePath.toString());
        } catch (Exception ex) {
            LauncherGetFileStatusMulti.LOG.severe(ex.getMessage());
            ex.printStackTrace();
        }
    }
}

class DownloadFileWithMD5 implements Runnable {
    Path hdfsFilePath = null;
    FileSystem fileSystemLocal = null;

    DownloadFileWithMD5(Path hdfsFilePath, FileSystem fileSystem) {
        this.hdfsFilePath = hdfsFilePath;
        this.fileSystemLocal = fileSystem;
    }

    public void run() {
        try {
            DistributedFileSystem dfs = (DistributedFileSystem) fileSystemLocal;
            long start = System.nanoTime();
            BufferedReader fBufRdr = null;
            FSDataInputStream fsDataInputStream = null;
            DigestInputStream digestInputStream = null;
            StringBuffer hexString = new StringBuffer();
            try {
                //fsDataInputStream = dfs.open(hdfsFilePath);
                //public FSDataInputStream open(Path f, final int bufferSize, List<ExtendedBlock> badBlocks)

                /*Constructor<ExtendedBlock> constructorExtendedBlock = ExtendedBlock.class.getConstructor(String.class,long.class);
                constructorExtendedBlock.newInstance(null,-9223372036854743616L);*/
                List<ExtendedBlock> extendedBlockList = new ArrayList<>();
                String[] blockIds = LauncherGetFileStatusMulti.blockids.split(",");
                Arrays.stream(blockIds).forEach( blockid -> {
                    ExtendedBlock extendedBlock = new ExtendedBlock(null,Long.valueOf(blockid));
                    extendedBlockList.add(extendedBlock);
                });
                //TBO extendedBlockList.forEach( blockid -> System.out.println("PRINT:blockid from list:"+blockid));
                MessageDigest messageDigest = MessageDigest.getInstance("MD5");

                /*Method andPrivateMethod
                        = DistributedFileSystem.class.getDeclaredMethod(
                        "open", Path.class, int.class,List.class);*/
                //fsDataInputStream = (FSDataInputStream) andPrivateMethod.invoke(dfs,hdfsFilePath,4096,extendedBlockList);
                fsDataInputStream = dfs.open(hdfsFilePath,1024,extendedBlockList);
                digestInputStream = new DigestInputStream(fsDataInputStream, messageDigest);
                fBufRdr = new BufferedReader(new InputStreamReader(digestInputStream));
                String line = fBufRdr.readLine();
                while (line != null) {
                    line = fBufRdr.readLine();
                }
                byte[] hash = messageDigest.digest();
                for (int i = 0; i < hash.length; i++) {
                    if ((0xff & hash[i]) < 0x10) {
                        hexString.append("0"
                                + Integer.toHexString((0xFF & hash[i])));
                    } else {
                        hexString.append(Integer.toHexString(0xFF & hash[i]));
                    }
                }
                //fBufRdr.close();
                //digestInputStream.close();
                //fsDataInputStream.close();
            } finally {
                if (fBufRdr != null) fBufRdr.close();
                if(digestInputStream != null) digestInputStream.close();
                if(fsDataInputStream != null) fsDataInputStream.close();
            }
            long end = System.nanoTime();
            long diffGet = end - start;
            if(LauncherGetFileStatusMulti.storeMetrics) {
                LauncherGetFileStatusMulti.totalResponseTimes.add(new Operation(Operation.OPERATIONS.GETMD5, System.nanoTime(), diffGet));
            }
            String flagDownloadFile = "";
            if (diffGet > LauncherGetFileStatusMulti.dnResponseTime) {
                flagDownloadFile = "HIGH";
            }
            LauncherGetFileStatusMulti.LOG.info("get took " + diffGet + " ns;" + flagDownloadFile +"md5sum:"+hexString);
        } catch (Exception ex) {
            LauncherGetFileStatusMulti.LOG.severe(ex.getMessage());
            ex.printStackTrace();
        }
    }
}