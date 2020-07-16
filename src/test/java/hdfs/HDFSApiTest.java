package hdfs;

import org.apache.commons.cli.ParseException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;


public class HDFSApiTest {
    private HDFSApi hdfsApi;

    @Before
    public void setUp() throws Exception {
        hdfsApi = new HDFSApi();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void copyToRemote() throws IOException {
        if (hdfsApi.exist("test1")) {
            hdfsApi.appendToFile("/Users/admin/Documents/javaprojects/HDFSDemo/pom.xml", "test1");
        } else {
            hdfsApi.copyFromFile("/Users/admin/Documents/javaprojects/HDFSDemo/pom.xml", "test1");
        }
    }

    @Test
    public void getFile() throws IOException {
        hdfsApi.getFile("test", "localFile");
    }

    @Test
    public void cat() throws IOException {
        hdfsApi.catDfsFile("test");
    }

    @Test
    public void ls() throws IOException {
        hdfsApi.ls("input");
    }

    @Test
    public void lsDir() throws IOException {
        hdfsApi.lsDir("/user");
    }

    @Test
    public void createOrDelete() throws IOException, ParseException {
//        String args[] = {"-delete"};
        String args[] = {};
        hdfsApi.createOrDelete("wawa/yangkai", args);
    }
}