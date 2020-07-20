package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class MyFSDataInputStreamTest {
    public Configuration conf;

    @Before
    public void setUp() {
        conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://zjka-cpc-backend-bigdata-qa-01:9000");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("dfs.replication", "1");
    }

    @Test
    public void cat() throws Exception {
        String remotePath = "wawa/yangkai/x.txt";
        MyFSDataInputStream.cat(conf, remotePath);
    }

}
