package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.Utils;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.clients.S3DataClient;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.test.AbstractBinderBuilder;
import edu.utexas.tacc.tapis.files.test.RandomByteInputStream;
import edu.utexas.tacc.tapis.files.test.RandomByteInputStream.SizeUnit;
import edu.utexas.tacc.tapis.files.test.TestUtils;
import edu.utexas.tacc.tapis.shared.ssh.SshSessionPool;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.apache.commons.io.IOUtils;
import org.glassfish.hk2.api.ServiceLocator;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.StreamingOutput;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static edu.utexas.tacc.tapis.files.lib.services.FileOpsService.MAX_LISTING_SIZE;
import static edu.utexas.tacc.tapis.files.lib.services.FileOpsService.MoveCopyOperation.MOVE;
import static edu.utexas.tacc.tapis.files.lib.services.FileOpsService.MoveCopyOperation.COPY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@Test(groups = "integration")
public class FileOpsServiceTests {
    private static final Logger log  = LoggerFactory.getLogger(FileOpsServiceTests.class);
    private static final String JSON_TEST_PATH="edu/utexas/tacc/tapis/files/lib/clients/TestSystems.json";
    private static String devTenant = "dev";
    private static String testuser = "testuser";

    @BeforeClass
    public void beforeClass() {
        if(SshSessionPool.getInstance() == null) {
            SshSessionPool.init();
        }
    }

    @BeforeMethod
    public void beforeMethod() throws ServiceException, IOException {
        Map<String, TapisSystem> tapisSystemMap = TestUtils.readSystems(JSON_TEST_PATH);
        for(String key : tapisSystemMap.keySet()) {
            TapisSystem testSystem = tapisSystemMap.get(key);
            ServiceLocator locator = new AbstractBinderBuilder()
                    .mockPerms(TestUtils.permsMock_AllowModifyForSystem(devTenant, testuser, testSystem.getId()))
                    .mockSystemsCache(TestUtils.systemCacheMock_GetSystem(devTenant, testuser, testSystem))
                    .mockSystemsCacheNoAuth(TestUtils.systemCacheNoAuthMock_GetSystem(devTenant, testuser, testSystem))
                    .buildAsServiceLocator();

            // get service to test
            FileOpsService fileOpsService = locator.getService(FileOpsService.class);

            RemoteDataClientFactory remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
            IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testuser, testSystem);
            cleanupAll(fileOpsService, client, testSystem);
        }
    }


    // Data provider for all test systems except S3
    // Needed since S3 does not support hierarchical directories and we do not emulate such functionality
    @DataProvider(name= "testSystems")
    public Object[][] testSystemsDataProvider() throws IOException {
        Map<String, TapisSystem> tapisSystemMap = TestUtils.readSystems(JSON_TEST_PATH);
        Object[][] providedData = new Object[tapisSystemMap.size()][1];
        int i = 0;
        for(String systemName : tapisSystemMap.keySet()) {
            providedData[i][0] = tapisSystemMap.get(systemName);
            i++;
        };
        return providedData;
    }

    // Data provider for all test systems except S3
    // Needed since S3 does not support hierarchical directories and we do not emulate such functionality
    @DataProvider(name= "testSystemsNoS3")
    public Object[][] testSystemsNoS3DataProvider () throws IOException {
        Map<String, TapisSystem> tapisSystemMap = TestUtils.readSystems(JSON_TEST_PATH);
        List<Object []> providedData = new ArrayList<Object[]>();
        int i = 0;
        for(String systemName : tapisSystemMap.keySet()) {
            TapisSystem system = tapisSystemMap.get(systemName);
            if(!system.getSystemType().equals(SystemTypeEnum.S3)) {
                Object[] systemParam = new Object[1];
                systemParam[0] = system;
                providedData.add(systemParam);
            }
        };

        return providedData.toArray(new Object[providedData.size()][1]);
    }

    // Data provider for SSH system
    @DataProvider(name= "testSystemsSSH")
    public Object[][] testSystemsSSHDataProvider () throws IOException {
        Map<String, TapisSystem> tapisSystemMap = TestUtils.readSystems(JSON_TEST_PATH);
        List<Object []> providedData = new ArrayList<Object[]>();
        int i = 0;
        for(String systemName : tapisSystemMap.keySet()) {
            TapisSystem system = tapisSystemMap.get(systemName);
            if(system.getSystemType().equals(SystemTypeEnum.LINUX)) {
                Object[] systemParam = new Object[1];
                systemParam[0] = system;
                providedData.add(systemParam);
            }
        };

        return providedData.toArray(new Object[providedData.size()][1]);
    }

    @Test(dataProvider = "testSystems")
    public void testListingPath(TapisSystem testSystem) throws Exception
    {
        ServiceLocator locator = new AbstractBinderBuilder()
                .mockPerms(TestUtils.permsMock_AllowModifyForSystem(devTenant, testuser, testSystem.getId()))
                .mockSystemsCache(TestUtils.systemCacheMock_GetSystem(devTenant, testuser, testSystem))
                .mockSystemsCacheNoAuth(TestUtils.systemCacheNoAuthMock_GetSystem(devTenant, testuser, testSystem))
                .buildAsServiceLocator();

        // get service to test
        FileOpsService fileOpsService = locator.getService(FileOpsService.class);

        RemoteDataClientFactory remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testuser, testSystem);

        RandomByteInputStream inputStream = new RandomByteInputStream(1024, SizeUnit.BYTES, true);
        fileOpsService.upload(client,"test.txt", inputStream);
        List<FileInfo> listing = fileOpsService.ls(client,"test.txt", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_PATTERN);
        Assert.assertEquals(listing.size(), 1);
        fileOpsService.delete(client,"test.txt");
        Assert.assertThrows(NotFoundException.class, ()-> { fileOpsService.ls(client, "test.txt", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_PATTERN); });
    }

    @Test(dataProvider = "testSystems")
    public void testListingPathNested(TapisSystem testSystem) throws Exception
    {
        ServiceLocator locator = new AbstractBinderBuilder()
                .mockPerms(TestUtils.permsMock_AllowModifyForSystem(devTenant, testuser, testSystem.getId()))
                .mockSystemsCache(TestUtils.systemCacheMock_GetSystem(devTenant, testuser, testSystem))
                .mockSystemsCacheNoAuth(TestUtils.systemCacheNoAuthMock_GetSystem(devTenant, testuser, testSystem))
                .buildAsServiceLocator();

        // get service to test
        FileOpsService fileOpsService = locator.getService(FileOpsService.class);

        RemoteDataClientFactory remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testuser, testSystem);

        InputStream in = Utils.makeFakeFile(10*1024);
        fileOpsService.upload(client,"/dir1/dir2/test.txt", in);
        List<FileInfo> listing = fileOpsService.ls(client,"/dir1/dir2", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_PATTERN);
        Assert.assertEquals(listing.size(), 1);
        Assert.assertEquals(listing.get(0).getPath(), "dir1/dir2/test.txt");
    }


    // Test error cases for upload
    // Uploading to "/" or "" should throw an exception with a certain message.
    // Uploading to an existing directory should throw an exception with a certain message.
    // NOTE that S3 does not support directories so skip
    @Test(dataProvider = "testSystems")
    public void testUploadErrors(TapisSystem testSystem) throws Exception
    {
        ServiceLocator locator = new AbstractBinderBuilder()
                .mockPerms(TestUtils.permsMock_AllowModifyForSystem(devTenant, testuser, testSystem.getId()))
                .mockSystemsCache(TestUtils.systemCacheMock_GetSystem(devTenant, testuser, testSystem))
                .mockSystemsCacheNoAuth(TestUtils.systemCacheNoAuthMock_GetSystem(devTenant, testuser, testSystem))
                .buildAsServiceLocator();

        // get service to test
        FileOpsService fileOpsService = locator.getService(FileOpsService.class);

        RemoteDataClientFactory remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testuser, testSystem);

        if (!SystemTypeEnum.S3.equals(client.getSystemType())) fileOpsService.mkdir(client, "dir1");
        InputStream in = Utils.makeFakeFile(1024);

        boolean pass = false;
        try { fileOpsService.upload(client,"/", in); }
        catch (Exception e)
        {
            Assert.assertTrue(e.getMessage().contains("FILES_ERR_SLASH_PATH"));
            pass = true;
        }
        Assert.assertTrue(pass);

        pass = false;
        try { fileOpsService.upload(client,"", in); }
        catch (Exception e)
        {
            Assert.assertTrue(e.getMessage().contains("FILES_ERR_EMPTY_PATH"));
            pass = true;
        }
        Assert.assertTrue(pass);

        // Skip dir related tests for S3
        if (SystemTypeEnum.S3.equals(client.getSystemType())) return;

        pass = false;
        try { fileOpsService.upload(client,"dir1", in); }
        catch (Exception e)
        {
            Assert.assertTrue(e.getMessage().contains("FILES_ERR_UPLOAD_DIR"));
            pass = true;
        }
        Assert.assertTrue(pass);
        pass = false;
        try { fileOpsService.upload(client,"/dir1", in); }
        catch (Exception e)
        {
            Assert.assertTrue(e.getMessage().contains("FILES_ERR_UPLOAD_DIR"));
            pass = true;
        }
        Assert.assertTrue(pass);
    }



    @Test(dataProvider = "testSystems")
    public void testUploadAndDelete(TapisSystem testSystem) throws Exception
    {
        ServiceLocator locator = new AbstractBinderBuilder()
                .mockPerms(TestUtils.permsMock_AllowModifyForSystem(devTenant, testuser, testSystem.getId()))
                .mockSystemsCache(TestUtils.systemCacheMock_GetSystem(devTenant, testuser, testSystem))
                .mockSystemsCacheNoAuth(TestUtils.systemCacheNoAuthMock_GetSystem(devTenant, testuser, testSystem))
                .buildAsServiceLocator();

        // get service to test
        FileOpsService fileOpsService = locator.getService(FileOpsService.class);

        RemoteDataClientFactory remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testuser, testSystem);

        InputStream in = Utils.makeFakeFile(10*1024);
        fileOpsService.upload(client,"/dir1/dir2/test.txt", in);
        // List files after upload
        System.out.println("After upload: ");
        List<FileInfo> listing = fileOpsService.ls(client,"dir1/dir2/", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_PATTERN);
        for (FileInfo fi : listing) { System.out.println("Found file:"+ fi.getName() + " at path: " + fi.getPath()); }
        Assert.assertEquals(listing.size(), 1);
        Assert.assertEquals(listing.get(0).getPath(), "dir1/dir2/test.txt");
        fileOpsService.delete(client,"/dir1/dir2/test.txt");
        Assert.assertThrows(NotFoundException.class, ()-> { fileOpsService.ls(client, "/dir1/dir2/test.txt", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_PATTERN); });
    }

    @Test(dataProvider = "testSystemsNoS3")
    public void testUploadAndDeleteNested(TapisSystem testSystem) throws Exception {
        ServiceLocator locator = new AbstractBinderBuilder()
                .mockPerms(TestUtils.permsMock_AllowModifyForSystem(devTenant, testuser, testSystem.getId()))
                .mockSystemsCache(TestUtils.systemCacheMock_GetSystem(devTenant, testuser, testSystem))
                .mockSystemsCacheNoAuth(TestUtils.systemCacheNoAuthMock_GetSystem(devTenant, testuser, testSystem))
                .buildAsServiceLocator();

        // get service to test
        FileOpsService fileOpsService = locator.getService(FileOpsService.class);

        RemoteDataClientFactory remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testuser, testSystem);

        InputStream in = Utils.makeFakeFile(10*1024);
        fileOpsService.upload(client,"a/b/c/test.txt", in);
        List<FileInfo> listing = fileOpsService.ls(client,"/a/b/c/test.txt", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_PATTERN);
        Assert.assertEquals(listing.size(), 1);
        fileOpsService.delete(client,"/a/b/");
        Assert.assertThrows(NotFoundException.class, ()-> { fileOpsService.ls(client,"/a/b/c/test.txt", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_PATTERN); });
    }

    @Test(dataProvider = "testSystems")
    public void testUploadAndGet(TapisSystem testSystem) throws Exception {
        ServiceLocator locator = new AbstractBinderBuilder()
                .mockPerms(TestUtils.permsMock_AllowModifyForSystem(devTenant, testuser, testSystem.getId()))
                .mockSystemsCache(TestUtils.systemCacheMock_GetSystem(devTenant, testuser, testSystem))
                .mockSystemsCacheNoAuth(TestUtils.systemCacheNoAuthMock_GetSystem(devTenant, testuser, testSystem))
                .buildAsServiceLocator();

        // get service to test
        FileOpsService fileOpsService = locator.getService(FileOpsService.class);

        RemoteDataClientFactory remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testuser, testSystem);

        InputStream in = Utils.makeFakeFile(100*1024);
        fileOpsService.upload(client,"test.txt", in);

        List<FileInfo> listing = fileOpsService.ls(client, "/test.txt", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_PATTERN);
        Assert.assertEquals(listing.get(0).getSize(), 100*1024);
        InputStream out = fileOpsService.getAllBytes(TestUtils.getRRUser(devTenant, testuser), testSystem,"test.txt");
        byte[] output = IOUtils.toByteArray(out);
        Assert.assertEquals(output.length, 100 * 1024);
    }

    @Test(dataProvider = "testSystems")
    public void testMoveFile(TapisSystem testSystem) throws Exception {
        ServiceLocator locator = new AbstractBinderBuilder()
                .mockPerms(TestUtils.permsMock_AllowModifyForSystem(devTenant, testuser, testSystem.getId()))
                .mockSystemsCache(TestUtils.systemCacheMock_GetSystem(devTenant, testuser, testSystem))
                .mockSystemsCacheNoAuth(TestUtils.systemCacheNoAuthMock_GetSystem(devTenant, testuser, testSystem))
                .buildAsServiceLocator();

        // get service to test
        FileOpsService fileOpsService = locator.getService(FileOpsService.class);

        RemoteDataClientFactory remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testuser, testSystem);

        InputStream in = Utils.makeFakeFile(100*1024);
        fileOpsService.upload(client,"test1.txt", in);
        in.close();
        fileOpsService.moveOrCopy(client, MOVE, "test1.txt", "test2.txt");
        List<FileInfo> listing = fileOpsService.ls(client, "/", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_PATTERN);
        for (FileInfo fi : listing) { System.out.println("Found file:"+ fi.getName() + " at path: " + fi.getPath()); }
        Assert.assertEquals(listing.size(), 1);
        Assert.assertEquals(listing.get(0).getName(), "test2.txt");
    }

    // Test copy and list for all systems
    // Use no subdirectories so we can include S3 when checking the listing count.
    @Test(dataProvider = "testSystems")
    public void testCopyFiles(TapisSystem testSystem) throws Exception
    {
        ServiceLocator locator = new AbstractBinderBuilder()
                .mockPerms(TestUtils.permsMock_AllowModifyForSystem(devTenant, testuser, testSystem.getId()))
                .mockSystemsCache(TestUtils.systemCacheMock_GetSystem(devTenant, testuser, testSystem))
                .mockSystemsCacheNoAuth(TestUtils.systemCacheNoAuthMock_GetSystem(devTenant, testuser, testSystem))
                .buildAsServiceLocator();

        // get service to test
        FileOpsService fileOpsService = locator.getService(FileOpsService.class);

        RemoteDataClientFactory remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testuser, testSystem);

    /*
        Create the following files and directories:
          /test1.txt
          /dir1/test1.txt
          /dir2/test1.txt
     */
        InputStream in = Utils.makeFakeFile(100*1024);
        fileOpsService.upload(client,"test1.txt", in);
        in.close();
        in = Utils.makeFakeFile(100*1024);
        fileOpsService.upload(client,"/dir1/test1.txt", in);
        in.close();
        in = Utils.makeFakeFile(100*1024);
        fileOpsService.upload(client,"/dir2/test1.txt", in);
        in.close();
        // List files before copying
        System.out.println("Before copying: ");
        List<FileInfo> listing = fileOpsService.ls(client, "/", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_PATTERN);
        for (FileInfo fi : listing) { System.out.println("Found file:"+ fi.getName() + " at path: " + fi.getPath()); }
    /* Now copy files. Should end up with following:
          /test1.txt
          /dir1/test1.txt
          /dir1/test02.txt
          /dir1/test03.txt
          /dir1/test04.txt
          /dir2/test1.txt
          /dir2/test05.txt
          /dir2/test06.txt
          /dir2/test07.txt
          /dir2/test08.txt
     */
        fileOpsService.moveOrCopy(client, COPY, "test1.txt", "/dir1/test01.txt");
        fileOpsService.moveOrCopy(client, COPY, "test1.txt", "dir1/test02.txt");
        fileOpsService.moveOrCopy(client, COPY, "/dir1/test1.txt", "/dir1/test03.txt");
        fileOpsService.moveOrCopy(client, COPY, "dir1/test1.txt", "/dir1/test04.txt");
        fileOpsService.moveOrCopy(client, COPY, "/dir1/test1.txt", "/dir2/test05.txt");
        fileOpsService.moveOrCopy(client, COPY, "dir1/test1.txt", "/dir2/test06.txt");
        fileOpsService.moveOrCopy(client, COPY, "/dir1/test1.txt", "dir2/test07.txt");
        fileOpsService.moveOrCopy(client, COPY, "dir1/test1.txt", "dir2/test08.txt");
        // Check listing for /dir1 - 5 files
        System.out.println("After copying: list for /dir1");
        listing = fileOpsService.ls(client, "/dir1", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_PATTERN);
        for (FileInfo fi : listing) { System.out.println("Found file:"+ fi.getName() + " at path: " + fi.getPath()); }
        Assert.assertEquals(listing.size(), 5);
        // Check listing for /dir2 - 5 files
        System.out.println("After copying: list for /dir2");
        listing = fileOpsService.ls(client, "/dir2", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_PATTERN);
        for (FileInfo fi : listing) { System.out.println("Found file:"+ fi.getName() + " at path: " + fi.getPath()); }
        Assert.assertEquals(listing.size(), 5);
    }

    // Test copy and list for all systems except S3
    // We have subdirectories so cannot include S3 when checking the listing count.
    @Test(dataProvider = "testSystemsNoS3")
    public void testCopyFilesNested(TapisSystem testSystem) throws Exception
    {
        ServiceLocator locator = new AbstractBinderBuilder()
                .mockPerms(TestUtils.permsMock_AllowModifyForSystem(devTenant, testuser, testSystem.getId()))
                .mockSystemsCache(TestUtils.systemCacheMock_GetSystem(devTenant, testuser, testSystem))
                .mockSystemsCacheNoAuth(TestUtils.systemCacheNoAuthMock_GetSystem(devTenant, testuser, testSystem))
                .buildAsServiceLocator();

        // get service to test
        FileOpsService fileOpsService = locator.getService(FileOpsService.class);

        RemoteDataClientFactory remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testuser, testSystem);
    /*
        Create the following files and directories:
          /test1.txt
          /dir1/test1.txt
          /dir1/dir2/test1.txt
          /dir2/test1.txt
     */
        InputStream in = Utils.makeFakeFile(100*1024);
        fileOpsService.upload(client,"test1.txt", in);
        in.close();
        in = Utils.makeFakeFile(100*1024);
        fileOpsService.upload(client,"/dir1/test1.txt", in);
        in.close();
        in = Utils.makeFakeFile(100*1024);
        fileOpsService.upload(client,"/dir2/test1.txt", in);
        in.close();
        in = Utils.makeFakeFile(100*1024);
        fileOpsService.upload(client,"/dir1/dir2/test1.txt", in);
        in.close();
        // Before copying when listing "/" should have 3 items since listing is not recursive
        // 2 directories and 1 file
        System.out.println("Before copying: ");
        List<FileInfo> listing = fileOpsService.ls(client, "/", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_PATTERN);
        for (FileInfo fi : listing) { System.out.println("Found file:"+ fi.getName() + " at path: " + fi.getPath()); }
        Assert.assertEquals(listing.size(), 3);
    /* Now copy files. Should end up with following:
          /test1.txt
          /test01.txt
          /test02.txt
          /test03.txt
          /test04.txt
          /dir1/test1.txt
          /dir1/test05.txt
          /dir1/test06.txt
          /dir1/test09.txt
          /dir1/test10.txt
          /dir1/test11.txt
          /dir1/test12.txt
          /dir1/dir2/test1.txt
          /dir1/dir2/test07.txt
          /dir1/dir2/test08.txt
          /dir1/dir2/test13.txt
          /dir1/dir2/test14.txt
          /dir1/dir2/test15.txt
          /dir2/test1.txt
     */
        fileOpsService.moveOrCopy(client, COPY, "/test1.txt", "/test01.txt");
        fileOpsService.moveOrCopy(client, COPY, "test1.txt", "/test02.txt");
        fileOpsService.moveOrCopy(client, COPY, "/test1.txt", "test03.txt");
        fileOpsService.moveOrCopy(client, COPY, "test1.txt", "test04.txt");
        fileOpsService.moveOrCopy(client, COPY, "test1.txt", "/dir1/test05.txt");
        fileOpsService.moveOrCopy(client, COPY, "test1.txt", "dir1/test06.txt");
        fileOpsService.moveOrCopy(client, COPY, "test1.txt", "/dir1/dir2/test07.txt");
        fileOpsService.moveOrCopy(client, COPY, "test1.txt", "dir1/dir2/test08.txt");
        fileOpsService.moveOrCopy(client, COPY, "/dir1/test1.txt", "/dir1/test09.txt");
        fileOpsService.moveOrCopy(client, COPY, "dir1/test1.txt", "/dir1/test10.txt");
        fileOpsService.moveOrCopy(client, COPY, "/dir1/dir2/test1.txt", "/dir1/test11.txt");
        fileOpsService.moveOrCopy(client, COPY, "dir1/dir2/test1.txt", "/dir1/test12.txt");
        fileOpsService.moveOrCopy(client, COPY, "/dir1/dir2/test1.txt", "/dir1/dir2/test13.txt");
        fileOpsService.moveOrCopy(client, COPY, "dir1/dir2/test1.txt", "/dir1/dir2/test14.txt");
        fileOpsService.moveOrCopy(client, COPY, "dir2/test1.txt", "/dir1/dir2/test15.txt");
        // Check listing for /
        // 2 directories and 5 files
        System.out.println("After copying: list for / ");
        listing = fileOpsService.ls(client, "/", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_PATTERN);
        for (FileInfo fi : listing) { System.out.println("Found file:"+ fi.getName() + " at path: " + fi.getPath()); }
        Assert.assertEquals(listing.size(), 7);
        // Check listing for /dir1
        // 1 directory and 7 files
        System.out.println("After copying: list for /dir1");
        listing = fileOpsService.ls(client, "/dir1", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_PATTERN);
        for (FileInfo fi : listing) { System.out.println("Found file:"+ fi.getName() + " at path: " + fi.getPath()); }
        Assert.assertEquals(listing.size(), 8);
        // Check listing for /dir2
        // 0 directories and 1 file
        System.out.println("After copying: list for /dir2");
        listing = fileOpsService.ls(client, "/dir2", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_PATTERN);
        for (FileInfo fi : listing) { System.out.println("Found file:"+ fi.getName() + " at path: " + fi.getPath()); }
        Assert.assertEquals(listing.size(), 1);
        // Check listing for /dir1/dir2
        // 0 directories and 6 files
        System.out.println("After copying: list for /dir1/dir2");
        listing = fileOpsService.ls(client, "/dir1/dir2", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_PATTERN);
        for (FileInfo fi : listing) { System.out.println("Found file:"+ fi.getName() + " at path: " + fi.getPath()); }
        Assert.assertEquals(listing.size(), 6);
    }


    // Test copy one directory to another
    // We have subdirectories so cannot include S3 when checking the listing count.
//  @Test(dataProvider = "testSystemsNoS3")
    @Test(dataProvider = "testSystemsSSH")
    public void testCopyDirToDir(TapisSystem testSystem) throws Exception
    {
        ServiceLocator locator = new AbstractBinderBuilder()
                .mockPerms(TestUtils.permsMock_AllowModifyForSystem(devTenant, testuser, testSystem.getId()))
                .mockSystemsCache(TestUtils.systemCacheMock_GetSystem(devTenant, testuser, testSystem))
                .mockSystemsCacheNoAuth(TestUtils.systemCacheNoAuthMock_GetSystem(devTenant, testuser, testSystem))
                .buildAsServiceLocator();

        // get service to test
        FileOpsService fileOpsService = locator.getService(FileOpsService.class);

        RemoteDataClientFactory remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testuser, testSystem);

    /*
        Create the following files and directories:
          /archive
          /archive/test1.txt
          /Test
     */
        fileOpsService.mkdir(client, "Test");
        InputStream in = Utils.makeFakeFile(100*1024);
        fileOpsService.upload(client,"archive/test1.txt", in);
        in.close();
        // List files before copying
        System.out.println("Before copying dir to dir. list for /: ");
        List<FileInfo> listing = fileOpsService.lsRecursive(client, "/", false, 5, IRemoteDataClient.NO_PATTERN);
        for (FileInfo fi : listing) { System.out.println("Found file:"+ fi.getName() + " at path: " + fi.getPath()); }
        Assert.assertEquals(listing.size(), 3);
    /* Now copy files. Should end up with following:
          /archive
          /archive/test1.txt
          /Test
          /Test/archive
          /Test/archive/test1.txt
     */
        fileOpsService.moveOrCopy(client, COPY, "/archive", "/Test");
        // Check listing for /
        System.out.println("After copying dir to dir: list for /");
        listing = fileOpsService.lsRecursive(client, "/", false, 5, IRemoteDataClient.NO_PATTERN);
        for (FileInfo fi : listing) { System.out.println("Found file:"+ fi.getName() + " at path: " + fi.getPath()); }
        Assert.assertEquals(listing.size(), 5);
    }

    // Test move of one directory to another
    // We have subdirectories so cannot include S3 when checking the listing count.
    @Test(dataProvider = "testSystemsNoS3", groups = {"broken"})
    public void testMoveDirToDir(TapisSystem testSystem) throws Exception
    {
        ServiceLocator locator = new AbstractBinderBuilder()
                .mockPerms(TestUtils.permsMock_AllowModifyForSystem(devTenant, testuser, testSystem.getId()))
                .mockSystemsCache(TestUtils.systemCacheMock_GetSystem(devTenant, testuser, testSystem))
                .mockSystemsCacheNoAuth(TestUtils.systemCacheNoAuthMock_GetSystem(devTenant, testuser, testSystem))
                .buildAsServiceLocator();

        // get service to test
        FileOpsService fileOpsService = locator.getService(FileOpsService.class);

        RemoteDataClientFactory remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testuser, testSystem);

    /*
        Create the following files and directories:
          /archive
          /archive/test1.txt
          /Test
     */
        fileOpsService.mkdir(client, "Test");
        InputStream in = Utils.makeFakeFile(100*1024);
        fileOpsService.upload(client,"archive/test1.txt", in);
        in.close();
        // List files before copying
        System.out.println("Before moving dir to dir. list for /: ");
        List<FileInfo> listing = fileOpsService.lsRecursive(client, "/", false, 5, IRemoteDataClient.NO_PATTERN);
        for (FileInfo fi : listing) { System.out.println("Found file:"+ fi.getName() + " at path: " + fi.getPath()); }
        Assert.assertEquals(listing.size(), 3);
    /* Now move files. Should end up with following:
          /Test
          /Test/archive
          /Test/archive/test1.txt
     */
        fileOpsService.moveOrCopy(client, MOVE, "/archive", "/Test");
        // Check listing for /
        System.out.println("After moving dir to dir: list for /");
        listing = fileOpsService.lsRecursive(client, "/", false, 5, IRemoteDataClient.NO_PATTERN);
        for (FileInfo fi : listing) { System.out.println("Found file:"+ fi.getName() + " at path: " + fi.getPath()); }
        Assert.assertEquals(listing.size(), 3);
    }
    @Test(dataProvider = "testSystems")
    public void testListing(TapisSystem testSystem) throws Exception
    {
        ServiceLocator locator = new AbstractBinderBuilder()
                .mockPerms(TestUtils.permsMock_AllowModifyForSystem(devTenant, testuser, testSystem.getId()))
                .mockSystemsCache(TestUtils.systemCacheMock_GetSystem(devTenant, testuser, testSystem))
                .mockSystemsCacheNoAuth(TestUtils.systemCacheNoAuthMock_GetSystem(devTenant, testuser, testSystem))
                .buildAsServiceLocator();

        // get service to test
        FileOpsService fileOpsService = locator.getService(FileOpsService.class);

        RemoteDataClientFactory remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testuser, testSystem);

        InputStream in = Utils.makeFakeFile(10*1024);
        fileOpsService.upload(client,"dir1/test1.txt", in);
        in.close();
        in = Utils.makeFakeFile(10*1024);
        fileOpsService.upload(client,"dir1/test2.txt", in);
        in.close();
        List<FileInfo> listing = fileOpsService.ls(client,"/dir1", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_PATTERN);
        for (FileInfo fi : listing) { System.out.println("Found file:"+ fi.getName() + " at path: " + fi.getPath()); }
        Assert.assertEquals(listing.size(), 2);
        String name1 = listing.get(0).getName();
        String name2 = listing.get(1).getName();
        Assert.assertTrue(name1.equals("test1.txt") || name1.equals("test2.txt"));
        Assert.assertTrue(name2.equals("test1.txt") || name2.equals("test2.txt"));
    }

    @Test(dataProvider = "testSystems")
    public void testUploadLargeFile(TapisSystem testSystem) throws Exception
    {
        ServiceLocator locator = new AbstractBinderBuilder()
                .mockPerms(TestUtils.permsMock_AllowModifyForSystem(devTenant, testuser, testSystem.getId()))
                .mockSystemsCache(TestUtils.systemCacheMock_GetSystem(devTenant, testuser, testSystem))
                .mockSystemsCacheNoAuth(TestUtils.systemCacheNoAuthMock_GetSystem(devTenant, testuser, testSystem))
                .buildAsServiceLocator();

        // get service to test
        FileOpsService fileOpsService = locator.getService(FileOpsService.class);

        RemoteDataClientFactory remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testuser, testSystem);

        InputStream in = Utils.makeFakeFile(100 * 1000 * 1024);
        fileOpsService.upload(client,"test.txt", in);
        List<FileInfo> listing = fileOpsService.ls(client,"test.txt", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_PATTERN);
        Assert.assertEquals(listing.size(), 1);
        Assert.assertEquals(listing.get(0).getName(), "test.txt");
        Assert.assertEquals(listing.get(0).getSize(), 100 * 1000 * 1024L);
    }

    @Test(dataProvider = "testSystems")
    public void testGetBytesByRange(TapisSystem testSystem) throws Exception
    {
        ServiceLocator locator = new AbstractBinderBuilder()
                .mockPerms(TestUtils.permsMock_AllowModifyForSystem(devTenant, testuser, testSystem.getId()))
                .mockSystemsCache(TestUtils.systemCacheMock_GetSystem(devTenant, testuser, testSystem))
                .mockSystemsCacheNoAuth(TestUtils.systemCacheNoAuthMock_GetSystem(devTenant, testuser, testSystem))
                .buildAsServiceLocator();

        // get service to test
        FileOpsService fileOpsService = locator.getService(FileOpsService.class);

        RemoteDataClientFactory remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testuser, testSystem);

        InputStream in = Utils.makeFakeFile( 1000 * 1024);
        fileOpsService.upload(client,"test.txt", in);
        InputStream result = fileOpsService.getByteRange(TestUtils.getRRUser(devTenant, testuser), testSystem,"test.txt", 0 , 1000);
        Assert.assertEquals(result.readAllBytes().length, 1000);
    }

    @Test(dataProvider = "testSystems")
    public void testGetFullStream(TapisSystem testSystem) throws Exception
    {
        ServiceLocator locator = new AbstractBinderBuilder()
                .mockPerms(TestUtils.permsMock_AllowModifyForSystem(devTenant, testuser, testSystem.getId()))
                .mockSystemsCache(TestUtils.systemCacheMock_GetSystem(devTenant, testuser, testSystem))
                .mockSystemsCacheNoAuth(TestUtils.systemCacheNoAuthMock_GetSystem(devTenant, testuser, testSystem))
                .buildAsServiceLocator();

        // get service to test
        FileOpsService fileOpsService = locator.getService(FileOpsService.class);

        RemoteDataClientFactory remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testuser, testSystem);

        InputStream in = Utils.makeFakeFile( 1000 * 1024);
        fileOpsService.upload(client,"test.txt", in);
        StreamingOutput streamingOutput = fileOpsService.getFullStream(TestUtils.getRRUser(devTenant, testuser), testSystem.getId(),"test.txt", null, null);
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        streamingOutput.write(outStream);
        Assert.assertEquals(outStream.size(), 1000*1024);
    }

    @Test(dataProvider = "testSystems")
    public void testGetZip(TapisSystem testSystem) throws Exception
    {
        ServiceLocator locator = new AbstractBinderBuilder()
                .mockPerms(TestUtils.permsMock_AllowModifyForSystem(devTenant, testuser, testSystem.getId()))
                .mockSystemsCache(TestUtils.systemCacheMock_GetSystem(devTenant, testuser, testSystem))
                .mockSystemsCacheNoAuth(TestUtils.systemCacheNoAuthMock_GetSystem(devTenant, testuser, testSystem))
                .buildAsServiceLocator();

        // get service to test
        FileOpsService fileOpsService = locator.getService(FileOpsService.class);

        RemoteDataClientFactory remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testuser, testSystem);

        client.delete("/");
        fileOpsService.upload(client,"a/test1.txt", Utils.makeFakeFile( 1000 * 1024));
        fileOpsService.upload(client,"a/b/test2.txt", Utils.makeFakeFile(1000 * 1024));
        fileOpsService.upload(client,"a/b/test3.txt", Utils.makeFakeFile(1000 * 1024));

        File file = File.createTempFile("test", ".zip");
        OutputStream outputStream = new FileOutputStream(file);
        fileOpsService.getZip(TestUtils.getRRUser(devTenant, testuser), outputStream, testSystem, "/a");

        try (FileInputStream fis = new FileInputStream(file); ZipInputStream zis = new ZipInputStream(fis))
        {
            ZipEntry ze;
            int count = 0;
            while ( (ze = zis.getNextEntry()) != null)
            {
                log.info(ze.toString());
                count++;
            }
            // S3 will have 3 entries and others should have 4 (3 files + 1 dir)
            if (client instanceof S3DataClient) Assert.assertEquals(count, 3);
            else Assert.assertEquals(count, 4);
        }
        file.deleteOnExit();
    }

    // TODO Remove
    // Need to talk with stave about this to be sure, but I feel pretty confident that this test is no longer valid.  The method it's testing should have been
    // called only after an auth check was performed via getResolvedSysWithAuthCheck
    // TODO Remove
//    @Test(dataProvider = "testSystems")
//    public void testUploadNoAuthz(TapisSystem testSystem) throws Exception {
//        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(false);
//        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystem);
//        InputStream in = Utils.makeFakeFile(10*1024);
//
//        Assert.assertThrows(ForbiddenException.class, ()-> { fileOpsService.upload(client,"test.txt", in); });
//    }

    @Test(dataProvider = "testSystems")
    public void testListingNoAuthz(TapisSystem testSystem) throws Exception
    {
        ServiceLocator locator = new AbstractBinderBuilder()
                .mockPerms(TestUtils.permsMock_AllowModifyWithNoReadForSystem(devTenant, testuser, testSystem.getId()))
                .mockSystemsCache(TestUtils.systemCacheMock_GetSystem(devTenant, testuser, testSystem))
                .mockSystemsCacheNoAuth(TestUtils.systemCacheNoAuthMock_GetSystem(devTenant, testuser, testSystem))
                .buildAsServiceLocator();

        // get service to test
        FileOpsService fileOpsService = locator.getService(FileOpsService.class);

        RemoteDataClientFactory remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testuser, testSystem);

        client.delete("/");
        fileOpsService.upload(client,"/test.txt", Utils.makeFakeFile(10*1024));
        // MODIFY should imply read so ls should work
        fileOpsService.ls(TestUtils.getRRUser(devTenant, testuser), testSystem.getId(), "test.txt", MAX_LISTING_SIZE, 0, null, null, IRemoteDataClient.NO_PATTERN);

        // Without MODIFY or READ should fail
        FilePermsService permsServiceMock = locator.getService(FilePermsService.class);
        Mockito.reset(permsServiceMock);
        when(permsServiceMock.isPermitted(any(), any(), any(), any(), eq(FileInfo.Permission.MODIFY))).thenReturn(false);
        Assert.assertThrows(ForbiddenException.class, ()-> { fileOpsService.ls(TestUtils.getRRUser(devTenant, testuser), testSystem.getId(), "test.txt", MAX_LISTING_SIZE, 0, null, null, IRemoteDataClient.NO_PATTERN); });
    }

    // NoAuthz tests for mkdir, move, copy and delete
    @Test(dataProvider = "testSystems", groups = {"broken"})
    public void testNoAuthzMany(TapisSystem testSystem) throws Exception
    {
        ServiceLocator locator = new AbstractBinderBuilder()
                .mockPerms(TestUtils.permsMock_AllowModifyForSystem(devTenant, testuser, testSystem.getId()))
                .mockSystemsCache(TestUtils.systemCacheMock_GetSystem(devTenant, testuser, testSystem))
                .mockSystemsCacheNoAuth(TestUtils.systemCacheNoAuthMock_GetSystem(devTenant, testuser, testSystem))
                .buildAsServiceLocator();

        // get service to test
        FileOpsService fileOpsService = locator.getService(FileOpsService.class);

        RemoteDataClientFactory remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testuser, testSystem);

        // Create directories and files for tests
        client.delete("/");
        fileOpsService.upload(client,"/1.txt", Utils.makeFakeFile(10*1024));
        fileOpsService.upload(client,"/a/2.txt", Utils.makeFakeFile(10*1024));
        fileOpsService.upload(client,"/b/3.txt", Utils.makeFakeFile(10*1024));
        // Perform the tests
        FilePermsService permsServiceMock = locator.getService(FilePermsService.class);
        when(permsServiceMock.isPermitted(any(), any(), any(), any(), any())).thenReturn(false);
        Assert.assertThrows(ForbiddenException.class, ()-> { fileOpsService.moveOrCopy(TestUtils.getRRUser(devTenant, testuser), MOVE, testSystem.getId(), "/1.txt","/1new.txt"); });
        Assert.assertThrows(ForbiddenException.class, ()-> { fileOpsService.moveOrCopy(TestUtils.getRRUser(devTenant, testuser), MOVE, testSystem.getId(), "/1.txt","/a/1new.txt"); });
        Assert.assertThrows(ForbiddenException.class, ()-> { fileOpsService.moveOrCopy(TestUtils.getRRUser(devTenant, testuser), MOVE, testSystem.getId(), "/a/2.txt","/b/2new.txt"); });
        Assert.assertThrows(ForbiddenException.class, ()-> { fileOpsService.moveOrCopy(TestUtils.getRRUser(devTenant, testuser), MOVE, testSystem.getId(), "/1.txt","/1new.txt"); });
        Assert.assertThrows(ForbiddenException.class, ()-> { fileOpsService.moveOrCopy(TestUtils.getRRUser(devTenant, testuser), MOVE, testSystem.getId(), "/1.txt","/a/1new.txt"); });
        Assert.assertThrows(ForbiddenException.class, ()-> { fileOpsService.moveOrCopy(TestUtils.getRRUser(devTenant, testuser), MOVE, testSystem.getId(),"/a/2.txt","/b/2new.txt"); });
        Assert.assertThrows(ForbiddenException.class, ()-> { fileOpsService.delete(client,"/1.txt"); });
        Assert.assertThrows(ForbiddenException.class, ()-> { fileOpsService.delete(client,"/a/1.txt"); });
        Assert.assertThrows(ForbiddenException.class, ()-> { fileOpsService.delete(client,"/a"); });
    }

    @Test(dataProvider = "testSystems")
    public void testListingRecursive(TapisSystem testSystem) throws Exception
    {
        ServiceLocator locator = new AbstractBinderBuilder()
                .mockPerms(TestUtils.permsMock_AllowModifyForSystem(devTenant, testuser, testSystem.getId()))
                .mockSystemsCache(TestUtils.systemCacheMock_GetSystem(devTenant, testuser, testSystem))
                .mockSystemsCacheNoAuth(TestUtils.systemCacheNoAuthMock_GetSystem(devTenant, testuser, testSystem))
                .buildAsServiceLocator();

        // get service to test
        FileOpsService fileOpsService = locator.getService(FileOpsService.class);

        RemoteDataClientFactory remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testuser, testSystem);

        int maxDepth = 5;
        client.delete("/");
        fileOpsService.upload(client,"/1.txt", Utils.makeFakeFile(10*1024));
        fileOpsService.upload(client,"/a/2.txt", Utils.makeFakeFile(10*1024));
        fileOpsService.upload(client,"/a/b/3.txt", Utils.makeFakeFile(10*1024));
        fileOpsService.upload(client,"/a/b/c/4.txt", Utils.makeFakeFile(10*1024));
        fileOpsService.upload(client,"/a/b/c/5.txt", Utils.makeFakeFile(10*1024));

        List<FileInfo> listing = fileOpsService.lsRecursive(client,"/", false, maxDepth, IRemoteDataClient.NO_PATTERN);
        for (FileInfo fi : listing) { log.info("Test1 found: " + fi.getUrl()); }
        // S3 doesn't really do folders
        // Test1 S3 should have 4 entries and others should have 7 (4 files + 3 directories)
        if (testSystem.getSystemType() == SystemTypeEnum.S3) Assert.assertEquals(listing.size(), maxDepth);
        else Assert.assertEquals(listing.size(), 8);

        // Test2 S3 should have 3 entries and others should have 5 (3 files + 2 directories)
        listing = fileOpsService.lsRecursive(client,"/a", false, maxDepth, IRemoteDataClient.NO_PATTERN);
        for (FileInfo fi : listing) { log.info("Test2 found: " + fi.getUrl()); }
        // S3 doesn't really do folders
        // S3 should have 3 entries and others should have 5 (3 files + 2 directories)
        if (testSystem.getSystemType() == SystemTypeEnum.S3) Assert.assertEquals(listing.size(), 4);
        else Assert.assertEquals(listing.size(), 6);
    }

    @Test(dataProvider = "testSystems")
    public void testZeroByteInsert(TapisSystem testSystem) throws Exception {
        ServiceLocator locator = new AbstractBinderBuilder()
                .mockPerms(TestUtils.permsMock_AllowModifyForSystem(devTenant, testuser, testSystem.getId()))
                .mockSystemsCache(TestUtils.systemCacheMock_GetSystem(devTenant, testuser, testSystem))
                .mockSystemsCacheNoAuth(TestUtils.systemCacheNoAuthMock_GetSystem(devTenant, testuser, testSystem))
                .buildAsServiceLocator();

        // get service to test
        FileOpsService fileOpsService = locator.getService(FileOpsService.class);

        RemoteDataClientFactory remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testuser, testSystem);

        client.delete("/");
        fileOpsService.upload(client,"/1.txt", Utils.makeFakeFile(0));

        List<FileInfo> listing = fileOpsService.ls(client,"/", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_PATTERN);
        // S3 doesn't really do folders?
        Assert.assertEquals(listing.size(), 1);
    }

    // Utility method to remove all files/objects given the client. Need to handle S3
    private void cleanupAll(FileOpsService fileOpsService, IRemoteDataClient client, TapisSystem sys) throws ServiceException {
        if (SystemTypeEnum.S3.equals(sys.getSystemType())) {
            fileOpsService.delete(client, "");
        } else {
            fileOpsService.delete(client, "/");
        }
    }

}
