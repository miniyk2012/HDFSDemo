package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

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
