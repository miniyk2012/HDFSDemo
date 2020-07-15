package hdfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.FileInputStream;
import java.io.IOException;

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
    }
}
