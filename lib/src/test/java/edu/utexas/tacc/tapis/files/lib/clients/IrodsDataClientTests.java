package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.Utils;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.test.TestUtils;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.ws.rs.NotFoundException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

@Test(groups = "integration")
public class IrodsDataClientTests extends BaseDataClientTests<IrodsDataClient> {
    private static final String JSON_TEST_PATH="edu/utexas/tacc/tapis/files/lib/clients/TestSystems.json";

    public IrodsDataClientTests() {
        super(IrodsDataClientTests.JSON_TEST_PATH);
    }

    @Test
    public void testCopy() throws Exception
    {
        TapisSystem system = TestUtils.readSystem(JSON_TEST_PATH, getConfigSection());
        IrodsDataClient client = new IrodsDataClient("dev", "dev", system);
        client.upload("/a/b/test.txt", Utils.makeFakeFile(10));
        client.copy("/a/b/test.txt", "/new/test.txt");
        List<FileInfo> listing = client.ls("/new/test.txt");
        Assert.assertTrue(listing.size() > 0);
        listing = client.ls("/a/b/test.txt");
        Assert.assertTrue(listing.size() > 0);
    }

    @Test
    public void testMove() throws Exception
    {
        TapisSystem system = TestUtils.readSystem(JSON_TEST_PATH, getConfigSection());
        IrodsDataClient client = new IrodsDataClient("dev", "dev", system);
        client.upload("/a/b/test.txt", Utils.makeFakeFile(10));
        client.move("/a/b/test.txt", "/new/test.txt");
        List<FileInfo> listing = client.ls("/new/test.txt");
        Assert.assertTrue(listing.size() > 0);
        Assert.assertThrows(NotFoundException.class, ()-> {
            client.ls("/a/b/test.txt");
        });
    }

    @Test
    public void testMove2() throws Exception
    {
        TapisSystem system = TestUtils.readSystem(JSON_TEST_PATH, getConfigSection());
        IrodsDataClient client = new IrodsDataClient("dev", "dev", system);
        client.upload("test.txt", Utils.makeFakeFile(10));
        client.move("test.txt", "/newName");
        List<FileInfo> listing = client.ls("newName");
        Assert.assertTrue(listing.size() > 0);
        Assert.assertThrows(NotFoundException.class, ()->{
            client.ls("test.txt");
        });
    }

    @Test
    public void testMoveDestinationExists() throws Exception
    {
        TapisSystem system = TestUtils.readSystem(JSON_TEST_PATH, getConfigSection());
        IrodsDataClient client = new IrodsDataClient("dev", "dev", system);
        client.upload("/a/b/test.txt", Utils.makeFakeFile(10));
        client.upload("/new/test.txt", Utils.makeFakeFile(10));
        Assert.assertThrows(IOException.class, ()->{
            client.move("/a/b/test.txt", "/new/test.txt");
        });
    }

    @Test
    public void testMkdir() throws Exception
    {
        TapisSystem system = TestUtils.readSystem(JSON_TEST_PATH, getConfigSection());
        IrodsDataClient client = new IrodsDataClient("dev", "dev", system);
        client.mkdir("/a/b/c/");
        List<FileInfo> listing=  client.ls("/a/b/");
        Assert.assertEquals(listing.size(), 1);
        Assert.assertEquals(listing.get(0).getPath(), "a/b/c");
    }

    @Test
    public void testListing() throws Exception
    {
        TapisSystem system = TestUtils.readSystem(JSON_TEST_PATH, getConfigSection());
        IrodsDataClient client = new IrodsDataClient("dev", "dev", system);
        client.upload("/a/b/c/test.txt", Utils.makeFakeFile(10000));
        List<FileInfo> listing = client.ls("a");
        Assert.assertTrue(listing.size() > 0);
    }

    @Test
    public void testListingNested() throws Exception
    {
        TapisSystem system = TestUtils.readSystem(JSON_TEST_PATH, getConfigSection());
        IrodsDataClient client = new IrodsDataClient("dev", "dev", system);
        client.upload("/a/b/c/test.txt", Utils.makeFakeFile(10000));
        List<FileInfo> listing = client.ls("a/b/c/test.txt");
        Assert.assertTrue(listing.size() > 0);
        Assert.assertEquals(listing.get(0).getPath(), "a/b/c/test.txt");
    }

    @Test
    public void testListingNested2() throws Exception
    {
        TapisSystem system = TestUtils.readSystem(JSON_TEST_PATH, getConfigSection());
        IrodsDataClient client = new IrodsDataClient("dev", "dev", system);
        client.upload("/a/b/c/test.txt", Utils.makeFakeFile(10000));
        List<FileInfo> listing = client.ls("a/b/c/");
        Assert.assertTrue(listing.size() == 1);
        Assert.assertEquals(listing.get(0).getPath(), "a/b/c/test.txt");
    }

    @Test
    public void testDelete() throws Exception
    {
        TapisSystem system = TestUtils.readSystem(JSON_TEST_PATH, getConfigSection());
        IrodsDataClient client = new IrodsDataClient("dev", "dev", system);
        client.upload("/dir1/dir2/test.txt", Utils.makeFakeFile(10000));
        client.delete("/dir1/dir2/test.txt");
        Assert.assertEquals(client.ls("/dir1/dir2/").size(), 0);
    }

    @Test
    public void testDeleteNested() throws Exception
    {
        TapisSystem system = TestUtils.readSystem(JSON_TEST_PATH, getConfigSection());
        IrodsDataClient client = new IrodsDataClient("dev", "dev", system);
        client.upload("/test.txt", Utils.makeFakeFile(10000));
        client.upload("/a/b/c/test.txt", Utils.makeFakeFile(10000));

        client.delete("/");
        Assert.assertEquals(client.ls("/").size(), 0);
    }

    @Test
    public void testDeleteNested2() throws Exception
    {
        TapisSystem system = TestUtils.readSystem(JSON_TEST_PATH, getConfigSection());
        IrodsDataClient client = new IrodsDataClient("dev", "dev", system);
        client.upload("/test.txt", Utils.makeFakeFile(10000));
        client.upload("/a/b/c/test.txt", Utils.makeFakeFile(10000));
        client.upload("/a/b/c/test2.txt", Utils.makeFakeFile(10000));
        client.upload("/a/b/c/test3.txt", Utils.makeFakeFile(10000));

        client.delete("/a/b/c/");
        Assert.assertEquals(client.ls("/a/b/").size(), 0);
    }

    @Test
    public void testDeleteRoot() throws Exception
    {
        TapisSystem system = TestUtils.readSystem(JSON_TEST_PATH, getConfigSection());
        IrodsDataClient client = new IrodsDataClient("dev", "dev", system);
        client.upload("/test.txt", Utils.makeFakeFile(10000));
        client.upload("/a/b/c/test.txt", Utils.makeFakeFile(10000));
        client.upload("/a/b/c/test2.txt", Utils.makeFakeFile(10000));
        client.upload("/a/b/c/test3.txt", Utils.makeFakeFile(10000));

        client.delete("/");
        Assert.assertEquals(client.ls("/").size(), 0);
    }

    @Test
    public void testInsert() throws Exception
    {
        TapisSystem system = TestUtils.readSystem(JSON_TEST_PATH, getConfigSection());
        int fileSize = 2000000;
        IrodsDataClient client = new IrodsDataClient("dev", "dev", system);
        client.upload("/a/b/c/test.txt", Utils.makeFakeFile(fileSize));

        List<FileInfo> listing = client.ls("/a/b/c/");
        Assert.assertEquals(listing.size(), 1);
        Assert.assertEquals(listing.get(0).getSize(), fileSize);
        Assert.assertEquals(listing.get(0).getName(), "test.txt");
        Assert.assertEquals(listing.get(0).getPath(), "a/b/c/test.txt");
    }

    @Test
    public void testInsertAtRoot() throws Exception
    {
        TapisSystem system = TestUtils.readSystem(JSON_TEST_PATH, getConfigSection());
        IrodsDataClient client = new IrodsDataClient("dev", "dev", system);
        // make sure to start with a clean root directory
        client.delete("/");
        client.upload("/test.txt", Utils.makeFakeFile(10000));
        List<FileInfo> listing = client.ls("/");
        Assert.assertEquals(listing.size(), 1);
        Assert.assertEquals(listing.get(0).getSize(), 10000);
        Assert.assertEquals(listing.get(0).getName(), "test.txt");
        Assert.assertEquals(listing.get(0).getPath(), "test.txt");
    }

    @Test
    public void testGetStream() throws Exception
    {
        TapisSystem system = TestUtils.readSystem(JSON_TEST_PATH, getConfigSection());
        int fileSize = 100*1024;
        IrodsDataClient client = new IrodsDataClient("dev", "dev", system);
        byte[] input = Utils.makeFakeFile(fileSize).readAllBytes();
        //sanity check...
        Assert.assertEquals(input.length, fileSize);
        client.upload("/a/b/c/test.txt", new ByteArrayInputStream(input));
        try (InputStream stream = client.getStream("/a/b/c/test.txt"))
        {
            byte[] output = IOUtils.toByteArray(stream);
            Assert.assertEquals(output.length, fileSize);
            Assert.assertEquals(input, output);
        }
    }

    @Test
    public void testGetBytesByRange() throws Exception
    {
        TapisSystem system = TestUtils.readSystem(JSON_TEST_PATH, getConfigSection());
        int fileSize = 20;
        int byteCount = 10;
        IrodsDataClient client = new IrodsDataClient("dev", "dev", system);
        byte[] input = Utils.makeFakeFile(fileSize).readAllBytes();
        Assert.assertEquals(input.length, fileSize);
        client.upload("/a/b/c/test.txt", new ByteArrayInputStream(input));
        try (InputStream stream = client.getBytesByRange("/a/b/c/test.txt", 0, byteCount))
        {
            byte[] output = IOUtils.toByteArray(stream);
            Assert.assertEquals(output.length, byteCount);
            Assert.assertEquals(Arrays.copyOfRange(input, 0, byteCount), output);
        }
    }

    /**
     * This should check for the case when the startByte is greater than the length of
     * the actual file, which *should* return an empty array
     * @throws Exception
     */
    @Test
    public void testGetBytesByRangeOutOfBounds() throws Exception
    {
        TapisSystem system = TestUtils.readSystem(JSON_TEST_PATH, getConfigSection());
        int fileSize = 20;
        int byteCount = 200;
        IrodsDataClient client = new IrodsDataClient("dev", "dev", system);
        byte[] input = Utils.makeFakeFile(fileSize).readAllBytes();
        Assert.assertEquals(input.length, fileSize);
        client.upload("/a/b/c/test.txt", new ByteArrayInputStream(input));
        try (InputStream stream = client.getBytesByRange("/a/b/c/test.txt", 30, byteCount))
        {
            byte[] output = IOUtils.toByteArray(stream);
            Assert.assertEquals(output.length, 0);
        }
    }

    /**
     * This should check for the case when the startByte is greater than the length of
     * the actual file, which *should* return an empty array
     * @throws Exception
     */
    @Test
    public void testGetBytesByRangePastEnd() throws Exception
    {
        TapisSystem system = TestUtils.readSystem(JSON_TEST_PATH, getConfigSection());
        int fileSize = 100;
        int byteCount = 200;
        IrodsDataClient client = new IrodsDataClient("dev", "dev", system);
        byte[] input = Utils.makeFakeFile(fileSize).readAllBytes();
        Assert.assertEquals(input.length, fileSize);
        client.upload("/a/b/c/test.txt", new ByteArrayInputStream(input));
        try (InputStream stream = client.getBytesByRange("/a/b/c/test.txt", 50, byteCount))
        {
            byte[] output = IOUtils.toByteArray(stream);
            Assert.assertEquals(output.length, 50);
            //Should be the last 50 bytes of the input array
            Assert.assertEquals(output, Arrays.copyOfRange(input, 50, 100));
        }
    }
    @Override
    protected String getConfigSection() {
        return "irods_system";
    }

    @Override
    public IrodsDataClient createDataClient(String tenantName, String userName, TapisSystem system, SystemsCache systemsCache,
                                            String impersonationId, String sharedCtxGrantor) throws Exception {
        return new IrodsDataClient(tenantName, userName, system);
    }
}
