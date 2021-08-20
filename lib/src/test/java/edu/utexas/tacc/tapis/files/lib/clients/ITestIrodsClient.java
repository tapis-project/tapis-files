package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.Utils;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.systems.client.gen.model.AuthnEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
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
        system.setRootDir("/tempZone/home/dev/");
        system.setDefaultAuthnMethod(AuthnEnum.PASSWORD);
        Credential creds = new Credential();
        creds.accessKey("dev");
        creds.setPassword("dev");
        system.setAuthnCredential(creds);
    }

    @AfterMethod
    public void cleanUp() throws Exception {
        IrodsDataClient client = new IrodsDataClient("dev", "dev", system);
        client.delete("/");
    }


    @Test
    public void testCopy() throws Exception {
        IrodsDataClient client = new IrodsDataClient("dev", "dev", system);
        client.insert("/a/b/test.txt", Utils.makeFakeFile(10));
        client.copy("/a/b/test.txt", "/new/test.txt");
        List<FileInfo> listing = client.ls("/new/test.txt");
        Assert.assertTrue(listing.size() > 0);
        listing = client.ls("/a/b/test.txt");
        Assert.assertTrue(listing.size() > 0);
    }

    @Test
    public void testMove() throws Exception {
        IrodsDataClient client = new IrodsDataClient("dev", "dev", system);
        client.insert("/a/b/test.txt", Utils.makeFakeFile(10));
        client.move("/a/b/test.txt", "/new/test.txt");
        List<FileInfo> listing = client.ls("/new/test.txt");
        Assert.assertTrue(listing.size() > 0);
        listing = client.ls("/a/b/test.txt");
        Assert.assertTrue(listing.size() == 0);
    }


    @Test
    public void testMkdir() throws Exception {
        IrodsDataClient client = new IrodsDataClient("dev", "dev", system);
        client.mkdir("/a/b/c/");
        List<FileInfo> listing=  client.ls("/a/b/");
        Assert.assertEquals(listing.size(), 1);
        Assert.assertEquals(listing.get(0).getPath(), "a/b/c");
    }

    @Test
    public void testListing() throws Exception {
        IrodsDataClient client = new IrodsDataClient("dev", "dev", system);
        client.insert("/a/b/c/test.txt", Utils.makeFakeFile(10000));
        List<FileInfo> listing = client.ls("a");
        Assert.assertTrue(listing.size() > 0);
    }

    @Test
    public void testDelete() throws Exception {
        IrodsDataClient client = new IrodsDataClient("dev", "dev", system);
        client.insert("/test.txt", Utils.makeFakeFile(10000));
        client.delete("/");
        Assert.assertEquals(client.ls("/").size(), 0);
    }

    @Test
    public void testDeleteNested() throws Exception {
        IrodsDataClient client = new IrodsDataClient("dev", "dev", system);
        client.insert("/test.txt", Utils.makeFakeFile(10000));
        client.insert("/a/b/c/test.txt", Utils.makeFakeFile(10000));

        client.delete("/");
        Assert.assertEquals(client.ls("/").size(), 0);
    }

    @Test
    public void testInsert() throws Exception {
        int fileSize = 2000000;
        IrodsDataClient client = new IrodsDataClient("dev", "dev", system);
        client.insert("/a/b/c/test.txt", Utils.makeFakeFile(fileSize));

        List<FileInfo> listing = client.ls("/a/b/c/");
        Assert.assertEquals(listing.size(), 1);
        Assert.assertEquals(listing.get(0).getSize(), fileSize);
        Assert.assertEquals(listing.get(0).getName(), "test.txt");
        Assert.assertEquals(listing.get(0).getPath(), "a/b/c/test.txt");
    }

    @Test
    public void testInsertAtRoot() throws Exception {
        IrodsDataClient client = new IrodsDataClient("dev", "dev", system);
        client.insert("/test.txt", Utils.makeFakeFile(10000));

        List<FileInfo> listing = client.ls("/");
        Assert.assertEquals(listing.size(), 1);
        Assert.assertEquals(listing.get(0).getSize(), 10000);
        Assert.assertEquals(listing.get(0).getName(), "test.txt");
        Assert.assertEquals(listing.get(0).getPath(), "test.txt");
    }

    @Test
    public void testGetStream() throws Exception {
        int fileSize = 2000000;
        IrodsDataClient client = new IrodsDataClient("dev", "dev", system);
        byte[] input = Utils.makeFakeFile(fileSize).readAllBytes();
        Assert.assertEquals(input.length, fileSize);
        client.insert("/a/b/c/test.txt", new ByteArrayInputStream(input));
        try (InputStream stream = client.getStream("/a/b/c/test.txt")) {
            byte[] output = IOUtils.toByteArray(stream);
            Assert.assertEquals(output.length, fileSize);
            Assert.assertEquals(input, output);
        }
    }


}
