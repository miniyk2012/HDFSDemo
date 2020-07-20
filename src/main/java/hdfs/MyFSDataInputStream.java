package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class MyFSDataInputStream extends FSDataInputStream {

    public MyFSDataInputStream(InputStream in) {
        super(in);
    }

    public static String readLine(BufferedReader reader) throws IOException {
        String line;
        if ((line = reader.readLine()) != null) {
            return line;
        }
        return null;
    }

    public static void cat(Configuration conf, String remotePath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(remotePath);
        BufferedReader bf = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line;
        while ((line=MyFSDataInputStream.readLine(bf)) != null) {
            System.out.println(line);
        }
    }
}
