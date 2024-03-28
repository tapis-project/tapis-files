package edu.utexas.tacc.tapis.files.lib.services;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.utexas.tacc.tapis.files.lib.Utils;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCacheNoAuth;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.clients.S3DataClient;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.factories.ServiceContextFactory;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.providers.ServiceClientsFactory;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService.MoveCopyOperation;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.shared.ssh.SshSessionPool;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHConnection;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.AuthnEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;
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
import javax.ws.rs.core.StreamingOutput;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import static edu.utexas.tacc.tapis.files.lib.services.FileOpsService.MAX_LISTING_SIZE;

/*
 These tests are designed to run against the ssh-machine image in the service's docker-compose file.
 The user is set to be testuser
*/
@Test(groups = {"integration"})
public class TestFileOpsService
{
  private final String devTenant = "dev";
  private final String testUser = "testuser";
  private final String nullImpersonationId = null;
  private final String sharedCtxGrantorNull = null;
  private ResourceRequestUser rTestUser;
  TapisSystem testSystemNotEnabled;
  TapisSystem testSystemSSH;
  TapisSystem testSystemS3;
  TapisSystem testSystemPKI;
  TapisSystem testSystemIrods;
  private RemoteDataClientFactory remoteDataClientFactory;
  private FileOpsService fileOpsService;
  private static final Logger log  = LoggerFactory.getLogger(TestFileOpsService.class);
  private final FilePermsService permsService = Mockito.mock(FilePermsService.class);
  private final SystemsCache systemsCache = Mockito.mock(SystemsCache.class);
  private final SystemsCacheNoAuth systemsCacheNoAuth = Mockito.mock(SystemsCacheNoAuth.class);

  private static final MoveCopyOperation OP_MV = MoveCopyOperation.MOVE;
  private static final MoveCopyOperation OP_CP = MoveCopyOperation.COPY;

  private TestFileOpsService() throws IOException
  {
    SSHConnection.setLocalNodeName("dev");
    String privateKey = IOUtils.toString(getClass().getResourceAsStream("/test-machine"), StandardCharsets.UTF_8);
    String publicKey = IOUtils.toString(getClass().getResourceAsStream("/test-machine.pub"), StandardCharsets.UTF_8);

    //SSH system with username/password
    Credential creds = new Credential();
    creds.setAccessKey(testUser);
    creds.setPassword("password");
    testSystemSSH = new TapisSystem();
    testSystemSSH.setTenant(devTenant);
    testSystemSSH.setId("testSystem");
    testSystemSSH.setSystemType(SystemTypeEnum.LINUX);
    testSystemSSH.setAuthnCredential(creds);
    testSystemSSH.setHost("localhost");
    testSystemSSH.setPort(2222);
    testSystemSSH.setRootDir("/data/home/testuser/");
    testSystemSSH.setDefaultAuthnMethod(AuthnEnum.PASSWORD);
    testSystemSSH.setEffectiveUserId(testUser);

    // PKI Keys system
    creds = new Credential();
    creds.setPublicKey(publicKey);
    creds.setPrivateKey(privateKey);
    testSystemPKI = new TapisSystem();
    testSystemPKI.setTenant(devTenant);
    testSystemPKI.setId("testSystem");
    testSystemPKI.setSystemType(SystemTypeEnum.LINUX);
    testSystemPKI.setAuthnCredential(creds);
    testSystemPKI.setHost("localhost");
    testSystemPKI.setPort(2222);
    testSystemPKI.setRootDir("/data/home/testuser/");
    testSystemPKI.setDefaultAuthnMethod(AuthnEnum.PKI_KEYS);
    testSystemPKI.setEffectiveUserId(testUser);

    //S3 system
    creds = new Credential();
    creds.setAccessKey("user");
    creds.setAccessSecret("password");
    testSystemS3 = new TapisSystem();
    testSystemS3.setTenant(devTenant);
    testSystemS3.setId("testSystem");
    testSystemS3.setSystemType(SystemTypeEnum.S3);
    testSystemS3.setHost("http://localhost");
    testSystemS3.setBucketName("test");
    testSystemS3.setPort(9000);
    testSystemS3.setAuthnCredential(creds);
    testSystemS3.setRootDir("");
    testSystemS3.setDefaultAuthnMethod(AuthnEnum.ACCESS_KEY);

    //Irods system
    creds = new Credential();
    creds.setLoginUser("dev");
    creds.setPassword("dev");
    testSystemIrods = new TapisSystem();
    testSystemIrods.setTenant(devTenant);
    testSystemIrods.setId("testSystem");
    testSystemIrods.setSystemType(SystemTypeEnum.IRODS);
    testSystemIrods.setHost("localhost");
    testSystemIrods.setPort(1247);
    testSystemIrods.setAuthnCredential(creds);
    testSystemIrods.setRootDir("/tempZone/home/dev/");
    testSystemIrods.setDefaultAuthnMethod(AuthnEnum.PASSWORD);
    testSystemIrods.setEffectiveUserId(creds.getLoginUser());

    //SSH system that is not enabled
    testSystemNotEnabled = new TapisSystem();
    testSystemNotEnabled.setId("testSystemNotEnabled");
    testSystemNotEnabled.setSystemType(SystemTypeEnum.LINUX);
    testSystemNotEnabled.setHost("localhost");
    testSystemNotEnabled.setPort(2222);
    testSystemNotEnabled.setRootDir("/data/home/testuser/");
    testSystemNotEnabled.setDefaultAuthnMethod(AuthnEnum.PASSWORD);
    testSystemNotEnabled.setEffectiveUserId(testUser);
    testSystemNotEnabled.setEnabled(false);
  }

  // Data provider for all test systems
    @DataProvider(name="testSystems")
    public Object[] testSystemsDataProvider ()
    {
      return new TapisSystem[] { testSystemSSH, testSystemS3, testSystemIrods, testSystemPKI };
    }

  // Data provider for all test systems except S3
  // Needed since S3 does not support hierarchical directories and we do not emulate such functionality
  @DataProvider(name= "testSystemsNoS3")
  public Object[] testSystemsNoS3DataProvider ()
  {
    return new TapisSystem[] { testSystemSSH, testSystemIrods, testSystemPKI };
  }

  // Data provider for NotEnabled system
  @DataProvider(name= "testSystemsNotEnabled")

  public Object[] testSystemsNotEnabled ()
  {
    return new TapisSystem[] { testSystemNotEnabled };
  }
  // Data provider for SSH system
  @DataProvider(name= "testSystemsSSH")
  public Object[] testSystemsSSHDataProvider ()
  {
    return new TapisSystem[] { testSystemSSH };
  }

  @BeforeSuite
  public void doBeforeSuite()
  {
    // Initialize TenantManager
    IRuntimeConfig settings = RuntimeSettings.get();
    String url = settings.getTenantsServiceURL();
    Map<String, Tenant> tenants = TenantManager.getInstance(url).getTenants();
    // Setup for dependency injection
    ServiceLocator locator = ServiceLocatorUtilities.createAndPopulateServiceLocator();
    ServiceLocatorUtilities.bind(locator, new AbstractBinder() {
      @Override
      protected void configure() {
        bindAsContract(RemoteDataClientFactory.class).in(Singleton.class);
        bindAsContract(FileOpsService.class).in(Singleton.class);
        bindAsContract(FileShareService.class).in(Singleton.class);
        bind(systemsCache).to(SystemsCache.class).ranked(1);
        bind(systemsCacheNoAuth).to(SystemsCacheNoAuth.class).ranked(1);
        bind(permsService).to(FilePermsService.class).ranked(1);
        bindFactory(ServiceClientsFactory.class).to(ServiceClients.class).in(Singleton.class);
        bindFactory(ServiceContextFactory.class).to(ServiceContext.class).in(Singleton.class);
      }
    });
    // Retrieving serviceContext does some important init stuff, see FilesApplication.java
    ServiceContext serviceContext = locator.getService(ServiceContext.class);
    ServiceClients serviceClients = locator.getService(ServiceClients.class);
    remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
    fileOpsService = locator.getService(FileOpsService.class);
// ????????????
//    ServiceLocatorUtilities.bind(locator, new AbstractBinder() {
//      @Override
//      protected void configure() {
//        bind(new SSHConnectionCache(5, TimeUnit.MINUTES)).to(SSHConnectionCache.class);
//        bindFactory(ServiceClientsFactory.class).to(ServiceClients.class).in(Singleton.class);
//        bindFactory(ServiceContextFactory.class).to(ServiceContext.class).in(Singleton.class);
//        bindAsContract(RemoteDataClientFactory.class).in(Singleton.class);
//        bindAsContract(FileOpsService.class).in(Singleton.class);
//        bindAsContract(FileShareService.class).in(Singleton.class);
//        bind(systemsCache).to(SystemsCache.class).ranked(1);
//        bind(permsService).to(FilePermsService.class).ranked(1);
//        bind(tenantManager).to(TenantManager.class);
//      }
//    });
//    remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
//    fileOpsService = locator.getService(FileOpsService.class);

    rTestUser = new ResourceRequestUser(new AuthenticatedUser(testUser, devTenant, TapisThreadContext.AccountType.user.name(),
            null, testUser, devTenant, null, null, null));
    FilePermsService.setSiteAdminTenantId("admin");
    FileShareService.setSiteAdminTenantId("admin");
  }

    @BeforeTest()
    public void setUp() throws Exception
    {
      Utils.clearSshSessionPoolInstance();
      SshSessionPool.init();
      when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
      IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystemSSH);
      try {
        when(systemsCache.getSystem(client.getOboTenant(), testSystemSSH.getId(), client.getOboUser(), nullImpersonationId, sharedCtxGrantorNull)).thenReturn(testSystemSSH);
        cleanupAll(client, testSystemSSH);
      } finally {
        reset(systemsCache);
      }
      client = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystemS3);
      cleanupAll(client, testSystemS3);
    }

    @AfterTest()
    public void tearDown() throws Exception
    {
      when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
      IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystemSSH);
      try {
        when(systemsCache.getSystem(devTenant, testSystemSSH.getId(), testUser, nullImpersonationId, sharedCtxGrantorNull)).thenReturn(testSystemSSH);
        cleanupAll(client, testSystemSSH);
      } finally {
        reset(systemsCache);
      }
      client = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystemS3);
      cleanupAll(client, testSystemS3);
      reset(systemsCache);
    }

  @Test(dataProvider = "testSystems")
  public void testListingPath(TapisSystem testSystem) throws Exception
  {
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
    IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystem);
    InputStream in = Utils.makeFakeFile(10*1024);
    fileOpsService.upload(client,"test.txt", in);
    List<FileInfo> listing = fileOpsService.ls(client,"test.txt", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_PATTERN);
    Assert.assertEquals(listing.size(), 1);
    fileOpsService.delete(client,"test.txt");
    Assert.assertThrows(NotFoundException.class, ()-> { fileOpsService.ls(client, "test.txt", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_PATTERN); });
  }

  @Test(dataProvider = "testSystems")
    public void testListingPathNested(TapisSystem testSystem) throws Exception
    {
      when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
      IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystem);
      cleanupAll(client, testSystem);
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
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
    IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystem);
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
      when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
      IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystem);
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
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystem);
        InputStream in = Utils.makeFakeFile(10*1024);
        fileOpsService.upload(client,"a/b/c/test.txt", in);
        List<FileInfo> listing = fileOpsService.ls(client,"/a/b/c/test.txt", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_PATTERN);
        Assert.assertEquals(listing.size(), 1);
        fileOpsService.delete(client,"/a/b/");
        Assert.assertThrows(NotFoundException.class, ()-> { fileOpsService.ls(client,"/a/b/c/test.txt", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_PATTERN); });
    }

    @Test(dataProvider = "testSystems")
    public void testUploadAndGet(TapisSystem testSystem) throws Exception {
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystem);
        cleanupAll(client, testSystem);
        InputStream in = Utils.makeFakeFile(100*1024);
        fileOpsService.upload(client,"test.txt", in);

        List<FileInfo> listing = fileOpsService.ls(client, "/test.txt", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_PATTERN);
        Assert.assertEquals(listing.get(0).getSize(), 100*1024);
        InputStream out = fileOpsService.getAllBytes(rTestUser, testSystem,"test.txt");
        byte[] output = IOUtils.toByteArray(out);
        Assert.assertEquals(output.length, 100 * 1024);
    }

  @Test(dataProvider = "testSystems")
  public void testMoveFile(TapisSystem testSystem) throws Exception {
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
    IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystem);
    cleanupAll(client, testSystem);
    InputStream in = Utils.makeFakeFile(100*1024);
    fileOpsService.upload(client,"test1.txt", in);
    in.close();
    fileOpsService.moveOrCopy(client, OP_MV, "test1.txt", "test2.txt");
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
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
    IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystem);
    /*
        Create the following files and directories:
          /test1.txt
          /dir1/test1.txt
          /dir2/test1.txt
     */
    cleanupAll(client, testSystem);
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
    fileOpsService.moveOrCopy(client, OP_CP, "test1.txt", "/dir1/test01.txt");
    fileOpsService.moveOrCopy(client, OP_CP, "test1.txt", "dir1/test02.txt");
    fileOpsService.moveOrCopy(client, OP_CP, "/dir1/test1.txt", "/dir1/test03.txt");
    fileOpsService.moveOrCopy(client, OP_CP, "dir1/test1.txt", "/dir1/test04.txt");
    fileOpsService.moveOrCopy(client, OP_CP, "/dir1/test1.txt", "/dir2/test05.txt");
    fileOpsService.moveOrCopy(client, OP_CP, "dir1/test1.txt", "/dir2/test06.txt");
    fileOpsService.moveOrCopy(client, OP_CP, "/dir1/test1.txt", "dir2/test07.txt");
    fileOpsService.moveOrCopy(client, OP_CP, "dir1/test1.txt", "dir2/test08.txt");
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
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
    IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystem);
    /*
        Create the following files and directories:
          /test1.txt
          /dir1/test1.txt
          /dir1/dir2/test1.txt
          /dir2/test1.txt
     */
    cleanupAll(client, testSystem);
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
    fileOpsService.moveOrCopy(client, OP_CP, "/test1.txt", "/test01.txt");
    fileOpsService.moveOrCopy(client, OP_CP, "test1.txt", "/test02.txt");
    fileOpsService.moveOrCopy(client, OP_CP, "/test1.txt", "test03.txt");
    fileOpsService.moveOrCopy(client, OP_CP, "test1.txt", "test04.txt");
    fileOpsService.moveOrCopy(client, OP_CP, "test1.txt", "/dir1/test05.txt");
    fileOpsService.moveOrCopy(client, OP_CP, "test1.txt", "dir1/test06.txt");
    fileOpsService.moveOrCopy(client, OP_CP, "test1.txt", "/dir1/dir2/test07.txt");
    fileOpsService.moveOrCopy(client, OP_CP, "test1.txt", "dir1/dir2/test08.txt");
    fileOpsService.moveOrCopy(client, OP_CP, "/dir1/test1.txt", "/dir1/test09.txt");
    fileOpsService.moveOrCopy(client, OP_CP, "dir1/test1.txt", "/dir1/test10.txt");
    fileOpsService.moveOrCopy(client, OP_CP, "/dir1/dir2/test1.txt", "/dir1/test11.txt");
    fileOpsService.moveOrCopy(client, OP_CP, "dir1/dir2/test1.txt", "/dir1/test12.txt");
    fileOpsService.moveOrCopy(client, OP_CP, "/dir1/dir2/test1.txt", "/dir1/dir2/test13.txt");
    fileOpsService.moveOrCopy(client, OP_CP, "dir1/dir2/test1.txt", "/dir1/dir2/test14.txt");
    fileOpsService.moveOrCopy(client, OP_CP, "dir2/test1.txt", "/dir1/dir2/test15.txt");
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
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
    IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystem);
    when(systemsCache.getSystem(client.getOboTenant(), testSystemSSH.getId(), client.getOboUser(), nullImpersonationId, sharedCtxGrantorNull)).thenReturn(testSystemSSH);
    /*
        Create the following files and directories:
          /archive
          /archive/test1.txt
          /Test
     */
    cleanupAll(client, testSystem);
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
    fileOpsService.moveOrCopy(client, OP_CP, "/archive", "/Test");
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
    IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystem);
    when(systemsCache.getSystem(client.getOboTenant(), testSystemSSH.getId(), client.getOboUser(), nullImpersonationId, sharedCtxGrantorNull)).thenReturn(testSystemSSH);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
    /*
        Create the following files and directories:
          /archive
          /archive/test1.txt
          /Test
     */
    cleanupAll(client, testSystem);
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
    fileOpsService.moveOrCopy(client, OP_MV, "/archive", "/Test");
    // Check listing for /
    System.out.println("After moving dir to dir: list for /");
    listing = fileOpsService.lsRecursive(client, "/", false, 5, IRemoteDataClient.NO_PATTERN);
    for (FileInfo fi : listing) { System.out.println("Found file:"+ fi.getName() + " at path: " + fi.getPath()); }
    Assert.assertEquals(listing.size(), 3);
  }
  @Test(dataProvider = "testSystems")
  public void testListing(TapisSystem testSystem) throws Exception
  {
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
    IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystem);
    cleanupAll(client, testSystem);
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
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystem);
        cleanupAll(client, testSystem);
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
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystem);
        InputStream in = Utils.makeFakeFile( 1000 * 1024);
        fileOpsService.upload(client,"test.txt", in);
        InputStream result = fileOpsService.getByteRange(rTestUser, testSystem,"test.txt", 0 , 1000);
        Assert.assertEquals(result.readAllBytes().length, 1000);
    }

  @Test(dataProvider = "testSystems")
  public void testGetFullStream(TapisSystem testSystem) throws Exception
  {
    when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);
    when(systemsCacheNoAuth.getSystem(any(), any(), any())).thenReturn(testSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
    IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystem);
    InputStream in = Utils.makeFakeFile( 1000 * 1024);
    fileOpsService.upload(client,"test.txt", in);
    StreamingOutput streamingOutput = fileOpsService.getFullStream(rTestUser, testSystem.getId(),"test.txt", nullImpersonationId, sharedCtxGrantorNull);
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    streamingOutput.write(outStream);
    Assert.assertEquals(outStream.size(), 1000*1024);
  }

    @Test(dataProvider = "testSystems")
    public void testGetZip(TapisSystem testSystem) throws Exception
    {
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystem);
        client.delete("/");
        fileOpsService.upload(client,"a/test1.txt", Utils.makeFakeFile( 1000 * 1024));
        fileOpsService.upload(client,"a/b/test2.txt", Utils.makeFakeFile(1000 * 1024));
        fileOpsService.upload(client,"a/b/test3.txt", Utils.makeFakeFile(1000 * 1024));

        File file = File.createTempFile("test", ".zip");
        OutputStream outputStream = new FileOutputStream(file);
        fileOpsService.getZip(rTestUser, outputStream, testSystem, "/a");

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
      when(permsService.isPermitted(any(), any(), any(), any(), eq(FileInfo.Permission.MODIFY))).thenReturn(true);
      when(permsService.isPermitted(any(), any(), any(), any(), eq(FileInfo.Permission.READ))).thenReturn(false);
      IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystem);
      client.delete("/");
      fileOpsService.upload(client,"/test.txt", Utils.makeFakeFile(10*1024));
      // MODIFY should imply read so ls should work
      fileOpsService.ls(rTestUser, testSystem.getId(), "test.txt", MAX_LISTING_SIZE, 0, nullImpersonationId, sharedCtxGrantorNull, IRemoteDataClient.NO_PATTERN);
      // Without MODIFY or READ should fail
      when(permsService.isPermitted(any(), any(), any(), any(), eq(FileInfo.Permission.MODIFY))).thenReturn(false);
      Assert.assertThrows(ForbiddenException.class, ()-> { fileOpsService.ls(rTestUser, testSystem.getId(), "test.txt", MAX_LISTING_SIZE, 0, nullImpersonationId, sharedCtxGrantorNull, IRemoteDataClient.NO_PATTERN); });
    }

  // NoAuthz tests for mkdir, move, copy and delete
  @Test(dataProvider = "testSystems", groups = {"broken"})
  public void testNoAuthzMany(TapisSystem testSystem) throws Exception
  {
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
    IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystem);
    // Create directories and files for tests
    client.delete("/");
    fileOpsService.upload(client,"/1.txt", Utils.makeFakeFile(10*1024));
    fileOpsService.upload(client,"/a/2.txt", Utils.makeFakeFile(10*1024));
    fileOpsService.upload(client,"/b/3.txt", Utils.makeFakeFile(10*1024));
    // Perform the tests
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(false);
// NOTE: After refactoring, mkdir call using client no longer does perm checks.
//    Assert.assertThrows(ForbiddenException.class, ()-> { fileOpsService.mkdir(client,"newdir"); });
//    Assert.assertThrows(ForbiddenException.class, ()-> { fileOpsService.mkdir(client,"/newdir"); });
//    Assert.assertThrows(ForbiddenException.class, ()-> { fileOpsService.mkdir(client,"/a/newdir"); });
    Assert.assertThrows(ForbiddenException.class, ()-> { fileOpsService.moveOrCopy(client,OP_MV,"/1.txt","/1new.txt"); });
    Assert.assertThrows(ForbiddenException.class, ()-> { fileOpsService.moveOrCopy(client,OP_MV,"/1.txt","/a/1new.txt"); });
    Assert.assertThrows(ForbiddenException.class, ()-> { fileOpsService.moveOrCopy(client,OP_MV,"/a/2.txt","/b/2new.txt"); });
    Assert.assertThrows(ForbiddenException.class, ()-> { fileOpsService.moveOrCopy(client, OP_CP,"/1.txt","/1new.txt"); });
    Assert.assertThrows(ForbiddenException.class, ()-> { fileOpsService.moveOrCopy(client, OP_CP,"/1.txt","/a/1new.txt"); });
    Assert.assertThrows(ForbiddenException.class, ()-> { fileOpsService.moveOrCopy(client, OP_CP,"/a/2.txt","/b/2new.txt"); });
    Assert.assertThrows(ForbiddenException.class, ()-> { fileOpsService.delete(client,"/1.txt"); });
    Assert.assertThrows(ForbiddenException.class, ()-> { fileOpsService.delete(client,"/a/1.txt"); });
    Assert.assertThrows(ForbiddenException.class, ()-> { fileOpsService.delete(client,"/a"); });
  }

  @Test(dataProvider = "testSystems")
    public void testListingRecursive(TapisSystem testSystem) throws Exception
    {
      int maxDepth = 5;
      when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
      IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystem);
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
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystem);
        client.delete("/");
        fileOpsService.upload(client,"/1.txt", Utils.makeFakeFile(0));

        List<FileInfo> listing = fileOpsService.ls(client,"/", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_PATTERN);
        // S3 doesn't really do folders?
      Assert.assertEquals(listing.size(), 1);
    }

  // Utility method to remove all files/objects given the client. Need to handle S3
  void cleanupAll(IRemoteDataClient client, TapisSystem sys) throws ServiceException {
    if (SystemTypeEnum.S3.equals(sys.getSystemType())) {
      fileOpsService.delete(client, "");
    } else {
      fileOpsService.delete(client, "/");
    }
  }

  // This is mostly to serve as an example of how to deserialize FileInfo.FileType into a class
  // should the need ever arise in the future.
  static class FileInfoTestClass {
    @JsonProperty
    private FileInfo.FileType fileType;

    @JsonCreator
    public FileInfoTestClass(@JsonProperty("fileType") String fileType) {
      this.fileType = FileInfo.FileType.fromString(fileType);
    }

    public FileInfo.FileType getFileType() {
      return fileType;
    }

    public void setFileType(FileInfo.FileType fileType) {
      this.fileType = fileType;
    }
  }
  @Test
  public void testFileTypeObjectMapping() throws JsonProcessingException {

    ObjectMapper mapper = new ObjectMapper();
    String fileString = mapper.writeValueAsString(new FileInfoTestClass("file"));
    String dirString = mapper.writeValueAsString(new FileInfoTestClass("dir"));
    String linkString = mapper.writeValueAsString(new FileInfoTestClass("symbolic_link"));
    String otherString = mapper.writeValueAsString(new FileInfoTestClass("other"));
    String unknownString = mapper.writeValueAsString(new FileInfoTestClass("unknown"));

    FileInfoTestClass file = mapper.readValue(fileString, FileInfoTestClass.class);
    FileInfoTestClass dir = mapper.readValue(dirString, FileInfoTestClass.class);
    FileInfoTestClass link = mapper.readValue(linkString, FileInfoTestClass.class);
    FileInfoTestClass other = mapper.readValue(otherString, FileInfoTestClass.class);
    FileInfoTestClass unknown = mapper.readValue(unknownString, FileInfoTestClass.class);

    Assert.assertEquals(file.getFileType(), FileInfo.FileType.FILE);
    Assert.assertEquals(dir.getFileType(), FileInfo.FileType.DIR);
    Assert.assertEquals(link.getFileType(), FileInfo.FileType.SYMBOLIC_LINK);
    Assert.assertEquals(other.getFileType(), FileInfo.FileType.OTHER);
    Assert.assertEquals(unknown.getFileType(), FileInfo.FileType.UNKNOWN);
  }
}
