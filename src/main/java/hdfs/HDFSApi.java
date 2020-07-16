package hdfs;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;


import java.io.*;
import java.text.SimpleDateFormat;

public class HDFSApi {
    final static Logger logger = LoggerFactory.getLogger(HDFSApi.class);
    private Configuration conf;

    public HDFSApi() {
        conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://zjka-cpc-backend-bigdata-qa-01:9000");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("dfs.replication", "1");
    }

    public boolean exist(String filename) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        return fs.exists(new Path(filename));
    }

    /**
     * 追加文件内容
     *
     * @param src 本地文件
     * @param dst hdfs文件
     * @throws IOException
     */
    public void appendToFile(String src, String dst) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path remotePath = new Path(dst);
        FileInputStream input = new FileInputStream(src);
        FSDataOutputStream output = fs.append(remotePath);
        byte[] data = new byte[1024];
        int read = -1;
        while ((read = input.read(data)) > 0) {
            output.write(data, 0, read);
        }
        input.close();
        output.close();
        fs.close();
    }

    public void copyFromFile(String src, String dst) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path remotePath = new Path(dst);
        Path localPath = new Path(src);
        fs.copyFromLocalFile(localPath, remotePath);
        fs.close();
    }

    public void getFile(String remoteFilePath, String localFilePath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path remotePath = new Path(remoteFilePath);
        while (true) {
            if (new File(localFilePath).exists()) {
                localFilePath = "new" + localFilePath;
            } else {
                break;
            }
        }
        File localPath = new File(localFilePath);
        FileUtil.copy(fs, remotePath, localPath, false, conf);
        fs.close();
    }

    public void catDfsFile(String filename) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path remotePath = new Path(filename);
        if (exist(filename)) {
            FSDataInputStream input = fs.open(remotePath);
            BufferedReader d = new BufferedReader(new InputStreamReader(input));
            String line = null;
            while ((line = d.readLine()) != null) {
                System.out.println(line);
            }
        } else {
            logger.warn("文件不存在");
        }
    }

    public void ls(String path) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path remotePath = new Path(path);
        FileStatus[] fileStatuses = fs.listStatus(remotePath);
        for (FileStatus status : fileStatuses) {
            System.out.println("permission " + status.getPermission());
            System.out.println("len " + status.getLen());
            Long timestamp = status.getModificationTime();
            SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            System.out.println("modified  " + sf.format(timestamp));
            System.out.println("path " + status.getPath());
            System.out.println();
        }
    }

    public void lsDir(String dirPath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path remotePath = new Path(dirPath);
        RemoteIterator<LocatedFileStatus> it = fs.listFiles(remotePath, true);
        while (it.hasNext()) {
            LocatedFileStatus status = it.next();
            System.out.println("permission " + status.getPermission());
            System.out.println("len " + status.getLen());
            Long timestamp = status.getModificationTime();
            SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            System.out.println("modified  " + sf.format(timestamp));
            System.out.println("path " + status.getPath());
            System.out.println();
        }
    }

    public void createOrDelete(String filePath, String[] args) throws ParseException, IOException {
        FileSystem fs = FileSystem.get(conf);
        Path remotePath = new Path(filePath);
        Options options = new Options();
        options.addOption("delete", false, "delete file");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);
        if (cmd.hasOption("delete")) {
            boolean success = fs.delete(remotePath, false);
            if (success) {
                System.out.println("删除文件成功");
            } else {
                System.out.println("文件不存在");
            }
        } else {
            FSDataOutputStream out = fs.create(remotePath);
            System.out.println("文件创建成功");
            out.close();
        }
        fs.close();
    }
}
