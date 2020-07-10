package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HDFSFileIfExist {
	final static Logger logger = LoggerFactory.getLogger(HDFSFileIfExist.class);
	public static void main(String[] args){
		try{
			String fileName = "test";
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://zjka-cpc-backend-bigdata-qa-01:9000");
			conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
			FileSystem fs = FileSystem.get(conf);
			if(fs.exists(new Path(fileName))){
				logger.info("文件存在");
			}else{
				logger.info("文件不存在");
			}
			
		}catch (Exception e){
			e.printStackTrace();
		}
	}

}
