package edu.utexas.tacc.tapis.files.lib.services;


import edu.utexas.tacc.tapis.files.lib.Utils;
import edu.utexas.tacc.tapis.files.lib.caches.FilePermsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SSHConnectionCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.factories.ServiceContextFactory;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo.Permission;
import edu.utexas.tacc.tapis.files.lib.models.ShareInfo;
import edu.utexas.tacc.tapis.files.lib.providers.ServiceClientsFactory;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService.MoveCopyOperation;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
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
import org.testng.annotations.Test;

import javax.inject.Singleton;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotFoundException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/*
 * Test File sharing support.
 * Based on test TestFileOpsService.
 */
@Test(groups = {"integration"})
public class TestFileShareService
{
  private final String testUser = "testuser";
  private final String testUser2 = "testuser2";
  private final String testUser3 = "testuser3";
  private final String devTenant = "dev";
  private final String nullImpersonationId = null;
  private ResourceRequestUser rTestUser;
  private ResourceRequestUser rTestUser2;
  private ResourceRequestUser rTestUser3;
  TapisSystem testSystemSSH;
  private RemoteDataClientFactory remoteDataClientFactory;
  private FilePermsService permsService;
  private FileOpsService fileOpsService;
  private FileShareService fileShareService;
  private static final Logger log  = LoggerFactory.getLogger(TestFileShareService.class);
  private final SystemsCache systemsCache = Mockito.mock(SystemsCache.class);

  private static final MoveCopyOperation OP_MV = MoveCopyOperation.MOVE;
  private static final MoveCopyOperation OP_CP = MoveCopyOperation.COPY;

  private TestFileShareService()
  {
    SSHConnection.setLocalNodeName("dev");

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
        bind(FileOpsService.class).to(FileOpsService.class).in(Singleton.class);
        bind(FilePermsService.class).to(FilePermsService.class).in(Singleton.class);
        bind(FilePermsCache.class).to(FilePermsCache.class).in(Singleton.class);
        bindFactory(ServiceClientsFactory.class).to(ServiceClients.class).in(Singleton.class);
        bindFactory(ServiceContextFactory.class).to(ServiceContext.class).in(Singleton.class);
      }
    });
    // This does some important init stuff, see FilesApplication.java
    ServiceContext serviceContext = locator.getService(ServiceContext.class);
    ServiceClients serviceClients = locator.getService(ServiceClients.class);
    permsService = locator.getService(FilePermsService.class);
    remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
    fileOpsService = locator.getService(FileOpsService.class);
    fileShareService = locator.getService(FileShareService.class);
    // Explicitly set systemsCache and serviceClients for FileShareService.
    // Have not been able to get this to happen via dependency injection.
    fileShareService.setSystemsCache(systemsCache);
    fileShareService.setServiceClients(serviceClients);

    rTestUser = new ResourceRequestUser(new AuthenticatedUser(testUser, devTenant, TapisThreadContext.AccountType.user.name(),
            null, testUser, devTenant, null, null, null));
    rTestUser2 = new ResourceRequestUser(new AuthenticatedUser(testUser2, devTenant, TapisThreadContext.AccountType.user.name(),
            null, testUser2, devTenant, null, null, null));
    rTestUser3 = new ResourceRequestUser(new AuthenticatedUser(testUser3, devTenant, TapisThreadContext.AccountType.user.name(),
            null, testUser3, devTenant, null, null, null));
    }

    @BeforeTest()
    public void setUp() throws Exception
    {
      // Grant testUser full perms
      permsService.grantPermission(devTenant, testUser, testSystemSSH.getId(), "/", Permission.MODIFY);
      IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystemSSH, testUser);
      fileOpsService.delete(client,"/");
    }

    @AfterTest()
    public void tearDown() throws Exception
    {
      IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystemSSH, testUser);
      fileOpsService.delete(client,"/");
    }

  // ===============================================
  // Test basic sharing of a single path
  // ===============================================
  @Test
  public void testSharePath() throws Exception
  {
    // Set up the mocked systemsCache
    when(systemsCache.getSystem(any(), eq("testSystemSSH"), any())).thenReturn(testSystemSSH);

    // Get the system
    TapisSystem tmpSys = LibUtils.getSystemIfEnabled(rTestUser, systemsCache, "testSystemSSH");
    Assert.assertNotNull(tmpSys);
    String sysId = tmpSys.getId();

    // Path to file used in testing
    String filePathStr = "/dir1/dir2/test1.txt";

    // Cleanup from any previous runs
    // Note that deleting a file should also remove any shares, so this is actually also a test of unshare
    try { fileOpsService.delete(rTestUser, tmpSys, filePathStr); } catch (NotFoundException e) {}

    // Create file at path
    InputStream in = Utils.makeFakeFile(10*1024);
    fileOpsService.upload(rTestUser, tmpSys, filePathStr, in);

    // TODO Check that testUser can get the file and testUser2 cannot

    // Create set of users for sharing
    var userSet= Collections.singleton(testUser2);

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
