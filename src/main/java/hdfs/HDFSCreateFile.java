package hdfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSCreateFile {
    final static Logger logger = LoggerFactory.getLogger(HDFSCreateFile.class);

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://zjka-cpc-backend-bigdata-qa-01:9000");
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            FileSystem fs = FileSystem.get(conf);
            byte[] buff = "Hello World 杨恺牛逼".getBytes();
            String fileName = "test";
            FSDataOutputStream os = fs.create(new Path(fileName));
            os.write(buff, 0, buff.length);
            logger.info("Create:" + fileName);
            os.close();
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}