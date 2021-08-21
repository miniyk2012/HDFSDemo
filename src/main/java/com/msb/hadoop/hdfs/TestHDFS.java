package com.msb.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;


public class TestHDFS {
    public Configuration conf = null;
    public FileSystem fs = null;

    @Before
    public void conn() throws Exception {
//        System.setProperty("HADOOP_USER_NAME", "miniyk");
        conf = new Configuration(true);  //true
        conf.set("dfs.blocksize", "1048576");
        fs = FileSystem.get(URI.create("hdfs://mycluster/"), conf, "good");
    }

    @Test
    public void mkdir() throws Exception {

        Path dir = new Path("/user/good");
        if (fs.exists(dir)) {
            fs.delete(dir, true);
        }
        fs.mkdirs(dir);
    }

    @Test
    public void upload() throws Exception {
        InputStream input = new BufferedInputStream(new FileInputStream("./data/bigdata.txt"));
        Path outfile = new Path("/msb01/bigdata.txt");
        OutputStream output = fs.create(outfile);
        IOUtils.copyBytes(input, output, conf, true);
    }


    @Test
    public void download() throws Exception {
        Path inputFile = new Path("/msb01/out.txt");
        InputStream hdfsInput = new BufferedInputStream(fs.open(inputFile));
        FileOutputStream localOut = new FileOutputStream("./data/download.txt");
        IOUtils.copyBytes(hdfsInput, localOut, conf, true);
    }

    @Test
    public void block() throws Exception {
        Path file = new Path("/msb01/bigdata.txt");
        FileStatus fileStatus = fs.getFileStatus(file);
        // 获取某段文件区间所在的block
        BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        for (BlockLocation b : fileBlockLocations) {
            System.out.println(b);
        }
        /**
         * 0,1048576,node03,node04
         * 1048576,540319,node04,node02
         */
        // 计算向书记移动
        // 用户和程序读取的是文件这个级别, 并不知道有块的概念
        FSDataInputStream in = fs.open(file);
        in.seek(1048576);
        // 计算向数据移动后, 期望的是分治, 只读取自己关心的, 会寻找到对应的block(通过seek实现), 并具备距离的概念(优先和本地的DN获取数据--框架的默认机制)
        System.out.println((char) in.readByte());
        System.out.println((char) in.readByte());
        System.out.println((char) in.readByte());
        System.out.println((char) in.readByte());
        System.out.println((char) in.readByte());
        System.out.println((char) in.readByte());
        System.out.println((char) in.readByte());
        System.out.println((char) in.readByte());
        System.out.println((char) in.readByte());
        System.out.println((char) in.readByte());
        System.out.println((char) in.readByte());
        System.out.println((char) in.readByte());
        System.out.println((char) in.readByte());
        System.out.println((char) in.readByte());
    }

    @After
    public void close() throws Exception {
        this.fs.close();
    }
}
