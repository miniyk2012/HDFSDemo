package com.msb.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class TestHDFS {
    public Configuration conf = null;
    public FileSystem fs = null;

    @Before
    public void conn() throws Exception {
        System.setProperty("HADOOP_USER_NAME", "miniyk");
        conf = new Configuration(true);  //true
        fs = FileSystem.get(conf);
    }

    @Test
    public void mkdir() throws Exception {

        Path dir = new Path("/msb01/01");
        if (fs.exists(dir)) {
            fs.delete(dir, true);
        }
        fs.mkdirs(dir);

    }


    @After
    public void close() throws Exception {
        this.fs.close();
    }
}
