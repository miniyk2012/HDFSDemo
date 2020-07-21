package hdfs;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;


/**
 * 合并HDFS中的文件
 */
public class Merge {
    Path inputPath;
    Path outputPath;
    Configuration conf;
    FileSystem fsSource, fsDst;
    static final Logger logger = LoggerFactory.getLogger(HDFSCreateFile.class);

    public Merge(String input, String output) {
        inputPath = new Path(input);
        outputPath = new Path(output);
    }

    public static void main(String[] args) throws IOException {
        Merge merge = new Merge(".", "merge.txt");
        merge.doMerge();
    }

    public void doMerge() throws IOException {
        initConf();
        openFileSystem();
        mergeToFile();
    }

    private void mergeToFile() throws IOException {
        FileStatus[] sourceStatus = fsSource.listStatus(inputPath, new MyPathFilter(".*\\.abc"));
        FSDataOutputStream fsdos = fsDst.create(outputPath);
        PrintStream ps = new PrintStream(System.out);
        for (FileStatus sta : sourceStatus) {
            if (!sta.isFile()) {
                continue;
            }
            logger.info("Path: " + sta.getPath() + " size: " + sta.getLen() + " permission: " +
                    sta.getPermission() + " content: ");
            FSDataInputStream fsdis = fsSource.open(sta.getPath());
            byte[] data = new byte[1024];
            int read = -1;
            while ((read = fsdis.read(data)) > 0) {
                ps.write(data, 0, read);
                fsdos.write(data, 0, read);
            }
            fsdis.close();
        }
        fsdos.close();
        ps.close();
    }

    private void initConf() {
        conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://zjka-cpc-backend-bigdata-qa-01:9000");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("dfs.replication", "1");
    }

    private void openFileSystem() throws IOException {
        fsSource = FileSystem.get(URI.create(inputPath.toString()), conf);
        fsDst = FileSystem.get(URI.create(outputPath.toString()), conf);
    }
}
