package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/*
 doc: http://hbase.apache.org/1.2/apidocs/org/apache/hadoop/hbase/client/package-summary.html
（1）列出HBase所有的表的相关信息，例如表名；
（2）在终端打印出指定的表的所有记录数据；
（3）向已经创建好的表添加和删除指定的列族或列；
（4）清空指定的表的所有记录数据；
 (5) 统计表的行数
 */
public class HbaseHomework1 {
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;

    public static void main(String[] args) {
        initHbase();
//        exercise1();
        exercise2();
    }

    public static void initHbase() {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "zjka-cpc-backend-bigdata-qa-01");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void exercise1() {
        try {
            HTableDescriptor hTableDescriptors[] = admin.listTables();
            for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
                System.out.println("table:" + hTableDescriptor.getNameAsString());
                HColumnDescriptor[] columnFamilies = hTableDescriptor.getColumnFamilies();
                for (HColumnDescriptor columnDescriptor : columnFamilies) {
                    System.out.println("    family:" + columnDescriptor.getNameAsString());
                    System.out.println("    minVersion:" + columnDescriptor.getMinVersions());
                    System.out.println("    maxVersion:" + columnDescriptor.getMaxVersions());
                    // System.out.println(columnDescriptor.getValues());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void exercise2() {
        try {
            String tableName = "teacher";
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result res : resultScanner) {
                String rowKey = Bytes.toString(res.getRow());
                System.out.println("row key :" + rowKey);
                for (Cell cell : res.rawCells()) {
                    System.out.println();
                    System.out.println("Timetamp:" + cell.getTimestamp() + " ");
                    System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
                    System.out.println("row Name:" + new String(CellUtil.cloneQualifier(cell)) + " ");
                    System.out.println("value:" + new String(CellUtil.cloneValue(cell)) + " ");
                }
                System.out.println("---------------");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void exercise3() {

    }
}
