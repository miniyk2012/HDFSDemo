package hdfs;

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
}