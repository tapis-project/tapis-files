package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.Utils;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

@Test(groups = {"integration"})
public class ITestIrodsClient {

    private TapisSystem system;

    @BeforeTest
    public void init() {
        system = new TapisSystem();
        system.setHost("localhost");
        system.setPort(1247);
    }

    @Test
    public void testListing() throws Exception {
        IrodsDataClient client = new IrodsDataClient("dev", "dev", system);
        List<FileInfo> listing = client.ls("/");
        Assert.assertTrue(listing.size() > 0);
    }

    @Test
    public void testDelete() throws Exception {
        IrodsDataClient client = new IrodsDataClient("dev", "dev", system);
        client.insert("/test.txt", Utils.makeFakeFile(10000));
        List<FileInfo> listing = client.ls("/");
        Assert.assertEquals(listing.size(), 1);
        client.delete("/test.txt");
        Assert.assertEquals(client.ls("/").size(), 0);
    }

    @Test
    public void testInsert() throws Exception {
        IrodsDataClient client = new IrodsDataClient("dev", "dev", system);
        client.insert("/a/b/c/test.txt", Utils.makeFakeFile(10000));
        Assert.assertEquals(client.ls("/a/b/c/").size(), 1);
    }

    @Test
    public void testGetStream() throws Exception {
        IrodsDataClient client = new IrodsDataClient("dev", "dev", system);
        String input = new String(Utils.makeFakeFile(10000).readAllBytes(), StandardCharsets.UTF_8);

        client.insert("/a/b/c/test.txt", new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)));
        try (InputStream stream = client.getStream("/a/b/c/test.txt")) {
            String output = new String(stream.readAllBytes());
            Assert.assertEquals(input, output);
        };

    }


}
