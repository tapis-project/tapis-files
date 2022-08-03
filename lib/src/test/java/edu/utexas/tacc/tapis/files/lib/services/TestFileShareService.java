package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.Utils;
import edu.utexas.tacc.tapis.files.lib.caches.SSHConnectionCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.clients.S3DataClient;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.factories.ServiceContextFactory;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.ShareInfo;
import edu.utexas.tacc.tapis.files.lib.providers.ServiceClientsFactory;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService.MoveCopyOperation;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import static edu.utexas.tacc.tapis.files.lib.services.FileOpsService.MAX_LISTING_SIZE;

/*
 * Test File sharing support.
 * Based on test TestFileOpsService.
 */
@Test(groups = {"integration"})
public class TestFileShareService
{
  private final String testUser = "testuser";
  private final String testUser2 = "testuser2";
  private final String oboTenant = "dev";
  private final String oboUser = testUser;
  private final String oboUser2 = testUser2;
  private final String nullImpersonationId = null;
  private ResourceRequestUser rTestUser;
  private ResourceRequestUser rTestUser2;
  TapisSystem testSystemNotEnabled;
  TapisSystem testSystemSSH;
  TapisSystem testSystemS3;
  TapisSystem testSystemPKI;
  TapisSystem testSystemIrods;
  private RemoteDataClientFactory remoteDataClientFactory;
  private FileOpsService fileOpsService;
  private FileShareService fileShareService;
  private static final Logger log  = LoggerFactory.getLogger(TestFileShareService.class);
  private final FilePermsService permsService = Mockito.mock(FilePermsService.class);
  private final SystemsCache systemsCache = Mockito.mock(SystemsCache.class);

  private static final MoveCopyOperation OP_MV = MoveCopyOperation.MOVE;
  private static final MoveCopyOperation OP_CP = MoveCopyOperation.COPY;

  private TestFileShareService() throws IOException
  {
    SSHConnection.setLocalNodeName("dev");
    String privateKey = IOUtils.toString(getClass().getResourceAsStream("/test-machine"), StandardCharsets.UTF_8);
    String publicKey = IOUtils.toString(getClass().getResourceAsStream("/test-machine.pub"), StandardCharsets.UTF_8);

    //SSH system with username/password
    Credential creds = new Credential();
    creds.setAccessKey(testUser);
    creds.setPassword("password");
    testSystemSSH = new TapisSystem();
    testSystemSSH.setId("testSystemSSH");
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
    testSystemS3.setId("testSystem");
    testSystemS3.setSystemType(SystemTypeEnum.S3);
    testSystemS3.setHost("http://localhost");
    testSystemS3.setBucketName("test");
    testSystemS3.setPort(9000);
    testSystemS3.setAuthnCredential(creds);
    testSystemS3.setRootDir("/");
    testSystemS3.setDefaultAuthnMethod(AuthnEnum.ACCESS_KEY);

    //Irods system
    creds = new Credential();
    creds.setAccessKey("dev");
    creds.setAccessSecret("dev");
    testSystemIrods = new TapisSystem();
    testSystemIrods.setId("testSystem");
    testSystemIrods.setSystemType(SystemTypeEnum.IRODS);
    testSystemIrods.setHost("localhost");
    testSystemIrods.setPort(1247);
    testSystemIrods.setAuthnCredential(creds);
    testSystemIrods.setRootDir("/tempZone/home/dev/");
    testSystemIrods.setDefaultAuthnMethod(AuthnEnum.PASSWORD);

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

  // Data provider for single Linux System
  @DataProvider(name= "testSystemsSSH")
  public Object[] testSystemsSSH ()
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
        bind(new SSHConnectionCache(5, TimeUnit.MINUTES)).to(SSHConnectionCache.class);
        bindAsContract(RemoteDataClientFactory.class).in(Singleton.class);
        bindAsContract(FileShareService.class).in(Singleton.class);
        bind(systemsCache).to(SystemsCache.class).ranked(1);
        bind(permsService).to(FilePermsService.class).ranked(1);
        bind(FileOpsService.class).to(FileOpsService.class).in(Singleton.class);
        bindFactory(ServiceClientsFactory.class).to(ServiceClients.class).in(Singleton.class);
        bindFactory(ServiceContextFactory.class).to(ServiceContext.class).in(Singleton.class);
      }
    });
    // This does some important init stuff, see FilesApplication.java
    ServiceContext serviceContext = locator.getService(ServiceContext.class);
    ServiceClients serviceClients = locator.getService(ServiceClients.class);
    remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
    fileOpsService = locator.getService(FileOpsService.class);
    fileShareService = locator.getService(FileShareService.class);
    // Explicitly set systemsCache and serviceClients for FileShareService.
    // Have not been able to get this to happen via dependency injection.
    fileShareService.setSystemsCache(systemsCache);
    fileShareService.setServiceClients(serviceClients);

    rTestUser = new ResourceRequestUser(new AuthenticatedUser(oboUser, oboTenant, TapisThreadContext.AccountType.user.name(),
            null, oboUser, oboTenant, null, null, null));
    rTestUser2 = new ResourceRequestUser(new AuthenticatedUser(oboUser2, oboTenant, TapisThreadContext.AccountType.user.name(),
            null, oboUser2, oboTenant, null, null, null));
    }

    @BeforeTest()
    public void setUp() throws Exception
    {
      when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
      IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystemSSH, testUser);
      fileOpsService.delete(client,"/");
      client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystemS3, testUser);
      fileOpsService.delete(client, "/");
    }

    @AfterTest()
    public void tearDown() throws Exception
    {
      when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
      IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystemSSH, testUser);
      fileOpsService.delete(client,"/");
      client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystemS3, testUser);
      fileOpsService.delete(client, "/");
    }

  // ===============================================
  // Test basic sharing of a single path
  // ===============================================
  @Test(dataProvider = "testSystemsSSH")
  public void testSharePath(TapisSystem testSystem) throws Exception
  {
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
    when(systemsCache.getSystem(any(), eq("testSystemSSH"), any())).thenReturn(testSystemSSH);
    // Cleanup from any previous runs
    // Note that deleting a file should also remove any shares, so this is actually also a test of unshare
    IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystem, testUser);
    fileOpsService.delete(client, "/");
    // Create file at path
    String filePathStr = "/dir1/dir2/test1.txt";
    InputStream in = Utils.makeFakeFile(10*1024);
    fileOpsService.upload(client, filePathStr, in);

    // Create set of users for sharing
    var userSet= Collections.singleton(testUser2);

    // Get the system
    TapisSystem tmpSys = LibUtils.getSystemIfEnabled(rTestUser, systemsCache, "testSystemSSH");
    Assert.assertNotNull(tmpSys);
    String sysId = tmpSys.getId();

    // Cleanup. Need this because fileOpsService.delete(client, path) does not remove shares.
    //      removal of shares is in fileOpsService.delete(rUser, sys, path)
    fileShareService.removeAllSharesForPath(rTestUser, sysId, filePathStr);

    // Get shareInfo. Should be empty
    ShareInfo shareInfo = fileShareService.getShareInfo(rTestUser, sysId, filePathStr);
    Assert.assertNotNull(shareInfo);
    Assert.assertNotNull(shareInfo.getUserSet());
    Assert.assertFalse(shareInfo.isPublic());
    Assert.assertTrue(shareInfo.getUserSet().isEmpty());
    // Share file with testuser2
    fileShareService.sharePath(rTestUser, sysId, filePathStr, userSet);
    // Get shareInfo for file. Should be shared with 1 user, testuser2
    shareInfo = fileShareService.getShareInfo(rTestUser, sysId, filePathStr);
    Assert.assertNotNull(shareInfo);
    Assert.assertNotNull(shareInfo.getUserSet());
    Assert.assertFalse(shareInfo.isPublic());
    Assert.assertFalse(shareInfo.getUserSet().isEmpty());
    Assert.assertEquals(shareInfo.getUserSet().size(), 1);
    Assert.assertTrue(shareInfo.getUserSet().contains(testUser2));
    // Remove share.
    fileShareService.unSharePath(rTestUser, sysId, filePathStr, userSet);
    // Get shareInfo. Should be empty.
    shareInfo = fileShareService.getShareInfo(rTestUser, sysId, filePathStr);
    Assert.assertNotNull(shareInfo);
    Assert.assertNotNull(shareInfo.getUserSet());
    Assert.assertFalse(shareInfo.isPublic());
    Assert.assertTrue(shareInfo.getUserSet().isEmpty());
  }
}
