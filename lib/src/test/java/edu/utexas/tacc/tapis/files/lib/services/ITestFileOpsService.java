package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.Utils;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.clients.S3DataClient;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.caches.SSHConnectionCache;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHConnection;
import edu.utexas.tacc.tapis.systems.client.gen.model.AuthnEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.apache.commons.io.IOUtils;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.hk2.utilities.ServiceLocatorUtilities;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.inject.Singleton;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotFoundException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/*
 These tests are designed to run against the ssh-machine image in the service's docker-compose file.
 The user is set to be testuser
*/
@Test(groups = {"integration"})
public class ITestFileOpsService
{
    private final String oboTenant = "oboTenant";
    private final String oboUser = "oboUser";
    TapisSystem testSystemSSH;
    TapisSystem testSystemS3;
    TapisSystem testSystemPKI;
    TapisSystem testSystemIrods;
    private RemoteDataClientFactory remoteDataClientFactory;
    private FileOpsService fileOpsService;
    private static final Logger log  = LoggerFactory.getLogger(ITestFileOpsService.class);
    private final FilePermsService permsService = Mockito.mock(FilePermsService.class);

    private ITestFileOpsService() throws IOException {
        SSHConnection.setLocalNodeName("dev");
        String privateKey = IOUtils.toString(
                this.getClass().getResourceAsStream("/test-machine"),
                StandardCharsets.UTF_8
        );
        String publicKey = IOUtils.toString(
                this.getClass().getResourceAsStream("/test-machine.pub"),
                StandardCharsets.UTF_8
        );
        //SSH system with username/password
        Credential creds = new Credential();
        creds.setAccessKey("testuser");
        creds.setPassword("password");
        testSystemSSH = new TapisSystem();
        testSystemSSH.setSystemType(SystemTypeEnum.LINUX);
        testSystemSSH.setAuthnCredential(creds);
        testSystemSSH.setHost("localhost");
        testSystemSSH.setPort(2222);
        testSystemSSH.setRootDir("/data/home/testuser/");
        testSystemSSH.setId("testSystem");
        testSystemSSH.setDefaultAuthnMethod(AuthnEnum.PASSWORD);
        testSystemSSH.setEffectiveUserId("testuser");

        // PKI Keys system
        creds = new Credential();
        creds.setPublicKey(publicKey);
        creds.setPrivateKey(privateKey);
        testSystemPKI = new TapisSystem();
        testSystemPKI.setSystemType(SystemTypeEnum.LINUX);
        testSystemPKI.setAuthnCredential(creds);
        testSystemPKI.setHost("localhost");
        testSystemPKI.setPort(2222);
        testSystemPKI.setRootDir("/data/home/testuser/");
        testSystemPKI.setId("testSystem");
        testSystemPKI.setDefaultAuthnMethod(AuthnEnum.PKI_KEYS);
        testSystemPKI.setEffectiveUserId("testuser");

        //S3 system
        creds = new Credential();
        creds.setAccessKey("user");
        creds.setAccessSecret("password");
        testSystemS3 = new TapisSystem();
        testSystemS3.setSystemType(SystemTypeEnum.S3);
        testSystemS3.setHost("http://localhost");
        testSystemS3.setBucketName("test");
        testSystemS3.setId("testSystem");
        testSystemS3.setPort(9000);
        testSystemS3.setAuthnCredential(creds);
        testSystemS3.setRootDir("/");
        testSystemS3.setDefaultAuthnMethod(AuthnEnum.ACCESS_KEY);

        //Irods system
        creds = new Credential();
        creds.setAccessKey("dev");
        creds.setAccessSecret("dev");
        testSystemIrods = new TapisSystem();
        testSystemIrods.setSystemType(SystemTypeEnum.IRODS);
        testSystemIrods.setHost("localhost");
        testSystemIrods.setId("testSystem");
        testSystemIrods.setPort(1247);
        testSystemIrods.setAuthnCredential(creds);
        testSystemIrods.setRootDir("/tempZone/home/dev/");
        testSystemIrods.setDefaultAuthnMethod(AuthnEnum.PASSWORD);
    }

    // Data provider for all test systems
    @DataProvider(name="testSystems")
    public Object[] testSystemsDataProvider () {
        return new TapisSystem[]{
                testSystemSSH,
                testSystemS3,
                testSystemIrods,
                testSystemPKI
        };
    }

  // Data provider for all test systems except S3
  // Needed since S3 does not support hierarchical directories and we do not emulate such functionality
  @DataProvider(name= "testSystemsNoS3")
  public Object[] testSystemsNoS3DataProvider () {
    return new TapisSystem[]{
            testSystemSSH,
            testSystemIrods,
            testSystemPKI
    };
  }

  @BeforeSuite
    public void doBeforeSuite() {
        ServiceLocator locator = ServiceLocatorUtilities.createAndPopulateServiceLocator();
        ServiceLocatorUtilities.bind(locator, new AbstractBinder() {
            @Override
            protected void configure() {
                bind(new SSHConnectionCache(10, 5, TimeUnit.MINUTES)).to(SSHConnectionCache.class);
                bindAsContract(RemoteDataClientFactory.class).in(Singleton.class);
                bind(permsService).to(FilePermsService.class).ranked(1);
                bind(FileOpsService.class).to(IFileOpsService.class).in(Singleton.class);
            }
        });
        remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
        fileOpsService = locator.getService(FileOpsService.class);
    }

    @BeforeTest()
    public void setUp() throws Exception
    {
      when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
      IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystemSSH, "testuser");
      fileOpsService.delete(client,"/");
      client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystemS3, "testuser");
      fileOpsService.delete(client, "/");
    }

    @AfterTest()
    public void tearDown() throws Exception
    {
      when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
      IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystemSSH, "testuser");
      fileOpsService.delete(client,"/");
      client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystemS3, "testuser");
      fileOpsService.delete(client, "/");
    }

    @Test(dataProvider = "testSystems")
    public void testListingPath(TapisSystem testSystem) throws Exception {
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystem, "testuser");
        InputStream in = Utils.makeFakeFile(10*1024);
        fileOpsService.upload(client,"test.txt", in);
        List<FileInfo> listing = fileOpsService.ls(client,"test.txt");
        Assert.assertEquals(listing.size(), 1);
        fileOpsService.delete(client,"test.txt");
        Assert.assertThrows(NotFoundException.class, ()-> {
            fileOpsService.ls(client, "test.txt");
        });
    }

    @Test(dataProvider = "testSystems")
    public void testListingPathNested(TapisSystem testSystem) throws Exception
    {
      when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
      IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystem, "testuser");
      fileOpsService.delete(client, "/");
      InputStream in = Utils.makeFakeFile(10*1024);
      fileOpsService.upload(client,"/dir1/dir2/test.txt", in);
      List<FileInfo> listing = fileOpsService.ls(client,"/dir1/dir2");
      Assert.assertEquals(listing.size(), 1);
      Assert.assertEquals(listing.get(0).getPath(), "/dir1/dir2/test.txt");
    }


    @Test(dataProvider = "testSystems")
    public void testUploadAndDelete(TapisSystem testSystem) throws Exception
    {
      when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
      IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystem, "testuser");
      InputStream in = Utils.makeFakeFile(10*1024);
      fileOpsService.upload(client,"/dir1/dir2/test.txt", in);
      // List files after upload
      System.out.println("After upload: ");
      List<FileInfo> listing = fileOpsService.ls(client,"dir1/dir2/");
      for (FileInfo fi : listing) { System.out.println("Found file:"+ fi.getName() + " at path: " + fi.getPath()); }
      Assert.assertEquals(listing.size(), 1);
      Assert.assertEquals(listing.get(0).getPath(), "/dir1/dir2/test.txt");
      fileOpsService.delete(client,"/dir1/dir2/test.txt");
      Assert.assertThrows(NotFoundException.class, ()-> {
        fileOpsService.ls(client, "/dir1/dir2/test.txt");
      });
    }

    @Test(dataProvider = "testSystemsNoS3")
    public void testUploadAndDeleteNested(TapisSystem testSystem) throws Exception {
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystem, "testuser");
        InputStream in = Utils.makeFakeFile(10*1024);
        fileOpsService.upload(client,"a/b/c/test.txt", in);
        List<FileInfo> listing = fileOpsService.ls(client,"/a/b/c/test.txt");
        Assert.assertEquals(listing.size(), 1);
        fileOpsService.delete(client,"/a/b/");
        Assert.assertThrows(NotFoundException.class, ()-> {
            fileOpsService.ls(client,"/a/b/c/test.txt");
        });
    }

    @Test(dataProvider = "testSystems")
    public void testUploadAndGet(TapisSystem testSystem) throws Exception {
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystem, "testuser");
        fileOpsService.delete(client, "/");
        InputStream in = Utils.makeFakeFile(100*1024);
        fileOpsService.upload(client,"test.txt", in);

        List<FileInfo> listing = fileOpsService.ls(client, "/test.txt");
        Assert.assertEquals(listing.get(0).getSize(), 100*1024);
        InputStream out = fileOpsService.getStream(client,"test.txt");
        byte[] output = IOUtils.toByteArray(out);
        Assert.assertEquals(output.length, 100 * 1024);
    }

  @Test(dataProvider = "testSystems")
  public void testMoveFile(TapisSystem testSystem) throws Exception {
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
    IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystem, "testuser");
    fileOpsService.delete(client, "/");
    InputStream in = Utils.makeFakeFile(100*1024);
    fileOpsService.upload(client,"test1.txt", in);
    in.close();
    fileOpsService.move(client, "test1.txt", "test2.txt");
    List<FileInfo> listing = fileOpsService.ls(client, "/");
    for (FileInfo fi : listing) { System.out.println("Found file:"+ fi.getName() + " at path: " + fi.getPath()); }
    Assert.assertEquals(listing.size(), 1);
    Assert.assertEquals(listing.get(0).getName(), "test2.txt");
  }

  // Test copy and list for all systems
  // Use no subdirectories so we can include S3 when checking the listing count.
  @Test(dataProvider = "testSystems")
  public void testCopyFiles(TapisSystem testSystem) throws Exception
  {
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
    IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystem, "testuser");
    /*
        Create the following files and directories:
          /test1.txt
          /dir1/test1.txt
          /dir2/test1.txt
     */
    fileOpsService.delete(client, "/");
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
    List<FileInfo> listing = fileOpsService.ls(client, "/");
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
    fileOpsService.copy(client, "test1.txt", "/dir1/test01.txt");
    fileOpsService.copy(client, "test1.txt", "dir1/test02.txt");
    fileOpsService.copy(client, "/dir1/test1.txt", "/dir1/test03.txt");
    fileOpsService.copy(client, "dir1/test1.txt", "/dir1/test04.txt");
    fileOpsService.copy(client, "/dir1/test1.txt", "/dir2/test05.txt");
    fileOpsService.copy(client, "dir1/test1.txt", "/dir2/test06.txt");
    fileOpsService.copy(client, "/dir1/test1.txt", "dir2/test07.txt");
    fileOpsService.copy(client, "dir1/test1.txt", "dir2/test08.txt");
    // Check listing for /dir1 - 5 files
    System.out.println("After copying: list for /dir1");
    listing = fileOpsService.ls(client, "/dir1");
    for (FileInfo fi : listing) { System.out.println("Found file:"+ fi.getName() + " at path: " + fi.getPath()); }
    Assert.assertEquals(listing.size(), 5);
    // Check listing for /dir2 - 5 files
    System.out.println("After copying: list for /dir2");
    listing = fileOpsService.ls(client, "/dir2");
    for (FileInfo fi : listing) { System.out.println("Found file:"+ fi.getName() + " at path: " + fi.getPath()); }
    Assert.assertEquals(listing.size(), 5);
  }

  // Test copy and list for all systems except S3
  // We have subdirectories so cannot include S3 when checking the listing count.
  @Test(dataProvider = "testSystemsNoS3")
  public void testCopyFilesNested(TapisSystem testSystem) throws Exception
  {
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
    IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystem, "testuser");
    /*
        Create the following files and directories:
          /test1.txt
          /dir1/test1.txt
          /dir1/dir2/test1.txt
          /dir2/test1.txt
     */
    fileOpsService.delete(client, "/");
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
    List<FileInfo> listing = fileOpsService.ls(client, "/");
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
    fileOpsService.copy(client, "/test1.txt", "/test01.txt");
    fileOpsService.copy(client, "test1.txt", "/test02.txt");
    fileOpsService.copy(client, "/test1.txt", "test03.txt");
    fileOpsService.copy(client, "test1.txt", "test04.txt");
    fileOpsService.copy(client, "test1.txt", "/dir1/test05.txt");
    fileOpsService.copy(client, "test1.txt", "dir1/test06.txt");
    fileOpsService.copy(client, "test1.txt", "/dir1/dir2/test07.txt");
    fileOpsService.copy(client, "test1.txt", "dir1/dir2/test08.txt");
    fileOpsService.copy(client, "/dir1/test1.txt", "/dir1/test09.txt");
    fileOpsService.copy(client, "dir1/test1.txt", "/dir1/test10.txt");
    fileOpsService.copy(client, "/dir1/dir2/test1.txt", "/dir1/test11.txt");
    fileOpsService.copy(client, "dir1/dir2/test1.txt", "/dir1/test12.txt");
    fileOpsService.copy(client, "/dir1/dir2/test1.txt", "/dir1/dir2/test13.txt");
    fileOpsService.copy(client, "dir1/dir2/test1.txt", "/dir1/dir2/test14.txt");
    fileOpsService.copy(client, "dir2/test1.txt", "/dir1/dir2/test15.txt");
    // Check listing for /
    // 2 directories and 5 files
    System.out.println("After copying: list for / ");
    listing = fileOpsService.ls(client, "/");
    for (FileInfo fi : listing) { System.out.println("Found file:"+ fi.getName() + " at path: " + fi.getPath()); }
    Assert.assertEquals(listing.size(), 7);
    // Check listing for /dir1
    // 1 directory and 7 files
    System.out.println("After copying: list for /dir1");
    listing = fileOpsService.ls(client, "/dir1");
    for (FileInfo fi : listing) { System.out.println("Found file:"+ fi.getName() + " at path: " + fi.getPath()); }
    Assert.assertEquals(listing.size(), 8);
    // Check listing for /dir2
    // 0 directories and 1 file
    System.out.println("After copying: list for /dir2");
    listing = fileOpsService.ls(client, "/dir2");
    for (FileInfo fi : listing) { System.out.println("Found file:"+ fi.getName() + " at path: " + fi.getPath()); }
    Assert.assertEquals(listing.size(), 1);
    // Check listing for /dir1/dir2
    // 0 directories and 6 files
    System.out.println("After copying: list for /dir1/dir2");
    listing = fileOpsService.ls(client, "/dir1/dir2");
    for (FileInfo fi : listing) { System.out.println("Found file:"+ fi.getName() + " at path: " + fi.getPath()); }
    Assert.assertEquals(listing.size(), 6);
  }

  @Test(dataProvider = "testSystems")
  public void testListing(TapisSystem testSystem) throws Exception
  {
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
    IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystem, "testuser");
    fileOpsService.delete(client, "/");
    InputStream in = Utils.makeFakeFile(10*1024);
    fileOpsService.upload(client,"dir1/test1.txt", in);
    in.close();
    in = Utils.makeFakeFile(10*1024);
    fileOpsService.upload(client,"dir1/test2.txt", in);
    in.close();
    List<FileInfo> listing = fileOpsService.ls(client,"/dir1");
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
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystem, "testuser");
        fileOpsService.delete(client, "/");
        InputStream in = Utils.makeFakeFile(100 * 1000 * 1024);
        fileOpsService.upload(client,"test.txt", in);
        List<FileInfo> listing = fileOpsService.ls(client,"test.txt");
        Assert.assertEquals(listing.size(), 1);
        Assert.assertEquals(listing.get(0).getName(), "test.txt");
        Assert.assertEquals(listing.get(0).getSize(), 100 * 1000 * 1024L);
    }

    @Test(dataProvider = "testSystems")
    public void testGetBytesByRange(TapisSystem testSystem) throws Exception {
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystem, "testuser");
        InputStream in = Utils.makeFakeFile( 1000 * 1024);
        fileOpsService.upload(client,"test.txt", in);
        InputStream result = fileOpsService.getBytes(client,"test.txt", 0, 1000);
        Assert.assertEquals(result.readAllBytes().length, 1000);
    }

    @Test(dataProvider = "testSystems")
    public void testZipFolder(TapisSystem testSystem) throws Exception {
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystem, "testuser");
        client.delete("/");
        fileOpsService.upload(client,"a/test1.txt", Utils.makeFakeFile( 1000 * 1024));
        fileOpsService.upload(client,"a/test2.txt", Utils.makeFakeFile(1000 * 1024));
        fileOpsService.upload(client,"a/b/test3.txt", Utils.makeFakeFile(1000 * 1024));

        File file = File.createTempFile("test", ".zip");
        OutputStream outputStream = new FileOutputStream(file);
        fileOpsService.getZip(client, outputStream, "/a");

        try (FileInputStream fis = new FileInputStream(file);
             ZipInputStream zis = new ZipInputStream(fis);
        ) {
            ZipEntry ze;
            int count = 0;
            while ( (ze = zis.getNextEntry()) != null) {
                log.info(ze.toString());
                String fname = ze.getName();
                count++;
            }
            // S3 will have 3 entries and others should have 4 (3 files + 1 dir)
            if (client instanceof S3DataClient) Assert.assertEquals(count, 3);
            else Assert.assertEquals(count, 4);
        }
        file.deleteOnExit();
    }


    @Test(dataProvider = "testSystems")
    public void testUploadNoAuthz(TapisSystem testSystem) throws Exception {
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(false);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystem, "testuser");
        InputStream in = Utils.makeFakeFile(10*1024);

        Assert.assertThrows(ForbiddenException.class, ()-> {
            fileOpsService.upload(client,"test.txt", in);
        });
    }

    @Test(dataProvider = "testSystems")
    public void testListingNoAuthz(TapisSystem testSystem) throws Exception {
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(false);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystem, "testuser");
        Assert.assertThrows(ForbiddenException.class, ()-> {
            fileOpsService.ls(client,"test.txt");
        });

    }

    @Test(dataProvider = "testSystems")
    public void testListingRecursive(TapisSystem testSystem) throws Exception {
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystem, "testuser");
        client.delete("/");
        fileOpsService.upload(client,"/1.txt", Utils.makeFakeFile(10*1024));
        fileOpsService.upload(client,"/a/2.txt", Utils.makeFakeFile(10*1024));
        fileOpsService.upload(client,"/a/b/3.txt", Utils.makeFakeFile(10*1024));
        fileOpsService.upload(client,"/a/b/c/4.txt", Utils.makeFakeFile(10*1024));

        List<FileInfo> listing = fileOpsService.lsRecursive(client,"/", 5);
        // S3 doesn't really do folders?
        Assert.assertTrue(listing.size() >=4);

    }

    @Test(dataProvider = "testSystems")
    public void testZeroByteInsert(TapisSystem testSystem) throws Exception {
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystem, "testuser");
        client.delete("/");
        fileOpsService.upload(client,"/1.txt", Utils.makeFakeFile(0));

        List<FileInfo> listing = fileOpsService.ls(client,"/");
        // S3 doesn't really do folders?
        Assert.assertTrue(listing.size() == 1);

    }
}
