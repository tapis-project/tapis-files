package edu.utexas.tacc.tapis.files.lib.services;


import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.files.lib.Utils;
import edu.utexas.tacc.tapis.files.lib.caches.FilePermsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SSHConnectionCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCacheNoAuth;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
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
import org.apache.commons.lang3.StringUtils;
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
  private final String testUser1 = "testuser";
  private final String testUser2 = "testuser2";
  private final String testUser3 = "testuser3";
  private final String devTenant = "dev";
  private final String siteId = "tacc";
  private final String nullImpersonationId = null;
  private final boolean sharedAppCtxFalse = false;
  private ResourceRequestUser rTestUser1;
  private ResourceRequestUser rTestUser2;
  private ResourceRequestUser rTestUser3;
  TapisSystem testSystemSSH;
  private RemoteDataClientFactory remoteDataClientFactory;
  private FilePermsService permsService;
  private FileOpsService fileOpsService;
  private FileShareService fileShareService;
  private static final Logger log  = LoggerFactory.getLogger(TestFileShareService.class);
  private final SystemsCache systemsCache = Mockito.mock(SystemsCache.class);
  private final SystemsCacheNoAuth systemsCacheNoAuth = Mockito.mock(SystemsCacheNoAuth.class);

  private static final MoveCopyOperation OP_MV = MoveCopyOperation.MOVE;
  private static final MoveCopyOperation OP_CP = MoveCopyOperation.COPY;

  private TestFileShareService()
  {
    SSHConnection.setLocalNodeName("dev");

    //SSH system with username/password
    Credential creds = new Credential();
    creds.setAccessKey(testUser1);
    creds.setPassword("password");
    testSystemSSH = new TapisSystem();
    testSystemSSH.setId("testSystemSSH");
    testSystemSSH.setOwner(testUser1);
    testSystemSSH.setTenant(devTenant);
    testSystemSSH.setSystemType(SystemTypeEnum.LINUX);
    testSystemSSH.setAuthnCredential(creds);
    testSystemSSH.setHost("localhost");
    testSystemSSH.setPort(2222);
    testSystemSSH.setRootDir("/data/home/testuser");
    testSystemSSH.setDefaultAuthnMethod(AuthnEnum.PASSWORD);
    testSystemSSH.setEffectiveUserId(testUser1);
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
        bindAsContract(FileOpsService.class).in(Singleton.class);
        bindAsContract(FileShareService.class).in(Singleton.class);
        bind(systemsCache).to(SystemsCache.class).ranked(1);
        bind(systemsCacheNoAuth).to(SystemsCacheNoAuth.class).ranked(1);
        bind(FilePermsService.class).to(FilePermsService.class).in(Singleton.class);
        bind(FilePermsCache.class).to(FilePermsCache.class).in(Singleton.class);
        bindFactory(ServiceClientsFactory.class).to(ServiceClients.class).in(Singleton.class);
        bindFactory(ServiceContextFactory.class).to(ServiceContext.class).in(Singleton.class);
      }
    });
    // Retrieving serviceContext does some important init stuff, see FilesApplication.java
    ServiceContext serviceContext = locator.getService(ServiceContext.class);
    ServiceClients serviceClients = locator.getService(ServiceClients.class);
    remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
    permsService = locator.getService(FilePermsService.class);
    fileOpsService = locator.getService(FileOpsService.class);
    fileShareService = locator.getService(FileShareService.class);

    rTestUser1 = new ResourceRequestUser(new AuthenticatedUser(testUser1, devTenant, TapisThreadContext.AccountType.user.name(),
            null, testUser1, devTenant, null, null, null));
    rTestUser2 = new ResourceRequestUser(new AuthenticatedUser(testUser2, devTenant, TapisThreadContext.AccountType.user.name(),
            null, testUser2, devTenant, null, null, null));
    rTestUser3 = new ResourceRequestUser(new AuthenticatedUser(testUser3, devTenant, TapisThreadContext.AccountType.user.name(),
            null, testUser3, devTenant, null, null, null));
    FilePermsService.setSiteAdminTenantId("admin");
    FileShareService.setSiteAdminTenantId("admin");
  }

    @BeforeTest()
    public void setUp() throws Exception
    {
      permsService.grantPermission(devTenant, testUser1, testSystemSSH.getId(), "/", Permission.MODIFY);
      IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser1, testSystemSSH, testUser1);
      fileOpsService.delete(client,"/");
    }

    @AfterTest()
    public void tearDown() throws Exception
    {
      permsService.grantPermission(devTenant, testUser1, testSystemSSH.getId(), "/", Permission.MODIFY);
      IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser1, testSystemSSH, testUser1);
      fileOpsService.delete(client,"/");
    }

  // ===============================================
  // Test basic sharing of various paths
  // ===============================================
  @Test
  public void testSharePaths() throws Exception
  {
    // Set up the mocked systemsCache
    when(systemsCache.getSystem(any(), eq("testSystemSSH"), any())).thenReturn(testSystemSSH);
    when(systemsCacheNoAuth.getSystem(any(), eq("testSystemSSH"), any())).thenReturn(testSystemSSH);

    // Get the system
    TapisSystem tmpSys = LibUtils.getSystemIfEnabled(rTestUser1, systemsCache, "testSystemSSH");
    Assert.assertNotNull(tmpSys);
    testSharingOfPath(tmpSys, "/", "test1.txt");
    testSharingOfPath(tmpSys, "", "test1.txt");
    testSharingOfPath(tmpSys, "/dir1/dir2/test1.txt", null);
  }

  // ===============================================
  // Test auth for sharing requests
  // ===============================================
  @Test
  public void testShareAuth() throws Exception
  {
    // Set up the mocked systemsCache
    when(systemsCache.getSystem(any(), eq("testSystemSSH"), any())).thenReturn(testSystemSSH);
    when(systemsCacheNoAuth.getSystem(any(), eq("testSystemSSH"), any())).thenReturn(testSystemSSH);

    // Get the system
    TapisSystem tmpSys = LibUtils.getSystemIfEnabled(rTestUser1, systemsCache, "testSystemSSH");
    Assert.assertNotNull(tmpSys);
    String sysId = tmpSys.getId();

    // Path to file used in testing
    String filePathStr = "/dir1/dir2/test2.txt";

    // Create set of users for sharing
    var userSet= Collections.singleton(testUser3);

    // Create file at path
    InputStream in = Utils.makeFakeFile(10*1024);
    fileOpsService.upload(rTestUser1, tmpSys, filePathStr, in);

    // Grant testUser1 full perms, testUser2 READ
    permsService.grantPermission(devTenant, testUser1, sysId, filePathStr, Permission.MODIFY);
    permsService.grantPermission(devTenant, testUser1, sysId, filePathStr, Permission.READ);
    permsService.grantPermission(devTenant, testUser2, sysId, filePathStr, Permission.READ);

    // Remove share from any previous failed run.
    fileShareService.removeAllSharesForPath(rTestUser1, sysId, filePathStr, true);
    // NOTE: If previous test failure resulted in share grants by user not system owner then
    //         it cannot be removed this way because SK uses oboUser to set grantor when removing a share.
    //         To clean up must remove perm check in FileShareService, add code here to remove shares
    //         using rTestUser2 or 3, make a run, then put back perm check in FileShareService.
    fileShareService.unSharePath(rTestUser1, sysId, filePathStr, userSet);

    // isSharedWithUser should return false
    Assert.assertFalse(fileShareService.isSharedWithUser(rTestUser1, tmpSys, filePathStr, testUser2));
    Assert.assertFalse(fileShareService.isSharedWithUser(rTestUser1, tmpSys, filePathStr, testUser3));

    // testUser2 has READ, so they should be able to get shareInfo
    fileShareService.getShareInfo(rTestUser2, sysId, filePathStr);

    // testUser3 does not have READ, so should not be able to get shareInfo
    boolean pass = false;
    try
    {
      fileShareService.getShareInfo(rTestUser3, sysId, filePathStr);
    }
    catch (ForbiddenException e) { pass = true; }
    Assert.assertTrue(pass, "User testUser3 should not be able to get share info");
    // testUser3 should not be able to create a share
    pass = false;
    try
    {
      fileShareService.sharePath(rTestUser3, sysId, filePathStr, userSet);
    }
    catch (ForbiddenException e) { pass = true; }
    Assert.assertTrue(pass, "User testUser3 should not be able to create share");

    // testUser2 is not system owner, so even with MODIFY should not be able to create a share.
    permsService.grantPermission(devTenant, testUser2, sysId, filePathStr, Permission.MODIFY);
    pass = false;
    try
    {
      fileShareService.sharePath(rTestUser2, sysId, filePathStr, userSet);
    }
    catch (ForbiddenException e) { pass = true; }
    Assert.assertTrue(pass, "User testUser2 should not be able to create share info");

    // Share with testUser3
    fileShareService.sharePath(rTestUser1, sysId, filePathStr, userSet);

    // TODO/TBD
//    // Now testUser3 should be able to get share info, and it should have only one user, themselves
//    ShareInfo shareInfo = fileShareService.getShareInfo(rTestUser3, sysId, filePathStr);
//    Assert.assertNotNull(shareInfo);
//    Assert.assertNotNull(shareInfo.getUserSet());
//    Assert.assertFalse(shareInfo.isPublic());
//    Assert.assertFalse(shareInfo.getUserSet().isEmpty());
//    Assert.assertEquals(shareInfo.getUserSet().size(), 1);
//    Assert.assertTrue(shareInfo.getUserSet().contains(testUser3));

    // isSharedWithUser should return true
    Assert.assertTrue(fileShareService.isSharedWithUser(rTestUser1, tmpSys, filePathStr, testUser3));

    // Remove share.
    fileShareService.unSharePath(rTestUser1, sysId, filePathStr, userSet);
    // Get shareInfo. Should be empty.
    ShareInfo shareInfo = fileShareService.getShareInfo(rTestUser1, sysId, filePathStr);
    Assert.assertNotNull(shareInfo);
    Assert.assertNotNull(shareInfo.getUserSet());
    Assert.assertFalse(shareInfo.isPublic());
    Assert.assertTrue(shareInfo.getUserSet().isEmpty());

    // isSharedWithUser should return false
    Assert.assertFalse(fileShareService.isSharedWithUser(rTestUser1, tmpSys, filePathStr, testUser2));
    Assert.assertFalse(fileShareService.isSharedWithUser(rTestUser1, tmpSys, filePathStr, testUser3));

    // Simple test of removeAllShares
    // Share with testUser3
    fileShareService.sharePath(rTestUser1, sysId, filePathStr, userSet);
    Assert.assertTrue(fileShareService.isSharedWithUser(rTestUser1, tmpSys, filePathStr, testUser3));
    fileShareService.removeAllSharesForPath(rTestUser1, sysId, filePathStr, false);
    Assert.assertFalse(fileShareService.isSharedWithUser(rTestUser1, tmpSys, filePathStr, testUser3));
  }

  // Test sharing of a path.
  // If fileToCreate is not blank then create the file.
  // If fileToCreate is blank then an attempt will be made to create a file using pathToShare
  private void testSharingOfPath(TapisSystem tSys, String pathToShare, String fileToCreate)
          throws ServiceException, TapisClientException
  {
    String sysId = tSys.getId();
    String fileToCheck;
    // Grant testUser full perms
    permsService.grantPermission(devTenant, testUser1, sysId, pathToShare, Permission.MODIFY);
    permsService.grantPermission(devTenant, testUser1, sysId, pathToShare, Permission.READ);

    // Create file at path or using filetToCreate
    InputStream in = Utils.makeFakeFile(1024);
    if (StringUtils.isBlank(fileToCreate))
    {
      fileOpsService.upload(rTestUser1, tSys, pathToShare, in);
      fileToCheck = pathToShare;
    }
    else
    {
      fileOpsService.upload(rTestUser1, tSys, fileToCreate, in);
      fileToCheck = pathToShare = "/" + fileToCreate;
    }

    // Clean up any previous shares using recurse=true
    fileShareService.removeAllSharesForPath(rTestUser1, sysId, pathToShare, true);

    // isSharedWithUser should return false
    Assert.assertFalse(fileShareService.isSharedWithUser(rTestUser2, tSys, pathToShare, testUser2));

    // Check that testUser can see the file and testUser2 cannot
    fileOpsService.ls(rTestUser1, tSys, pathToShare, 1, 0, nullImpersonationId, sharedAppCtxFalse);
    fileOpsService.ls(rTestUser1, tSys, fileToCheck, 1, 0, nullImpersonationId, sharedAppCtxFalse);
    fileOpsService.getFileInfo(rTestUser1, tSys, fileToCheck, nullImpersonationId, sharedAppCtxFalse);
    boolean pass = false;
    try
    {
      fileOpsService.ls(rTestUser2, tSys, pathToShare, 1, 0, nullImpersonationId, sharedAppCtxFalse);
    }
    catch (ForbiddenException e) { pass = true; }
    Assert.assertTrue(pass, "User testUser2 should not be able to list path");
    pass = false;
    try
    {
      fileOpsService.getFileInfo(rTestUser2, tSys, fileToCheck, nullImpersonationId, sharedAppCtxFalse);
    }
    catch (ForbiddenException e) { pass = true; }
    Assert.assertTrue(pass, "User testUser2 should not be able to getFileInfo");
    pass = false;
    try
    {
      fileOpsService.ls(rTestUser2, tSys, fileToCheck, 1 , 0, nullImpersonationId, sharedAppCtxFalse);
    }
    catch (ForbiddenException e) { pass = true; }
    Assert.assertTrue(pass, "User testUser2 should not be able to list path to file");

    // Create set of users for sharing
    var userSet= Collections.singleton(testUser2);

    // Get shareInfo. Should be empty
    ShareInfo shareInfo = fileShareService.getShareInfo(rTestUser1, sysId, pathToShare);
    Assert.assertNotNull(shareInfo);
    Assert.assertNotNull(shareInfo.getUserSet());
    Assert.assertFalse(shareInfo.isPublic());
    Assert.assertTrue(shareInfo.getUserSet().isEmpty());

    //
    // Share path with testuser2
    //
    fileShareService.sharePath(rTestUser1, sysId, pathToShare, userSet);
    // Get shareInfo for file. Should be shared with 1 user, testuser2
    shareInfo = fileShareService.getShareInfo(rTestUser1, sysId, pathToShare);
    Assert.assertNotNull(shareInfo);
    Assert.assertNotNull(shareInfo.getUserSet());
    Assert.assertFalse(shareInfo.isPublic());
    Assert.assertFalse(shareInfo.getUserSet().isEmpty());
    Assert.assertEquals(shareInfo.getUserSet().size(), 1);
    Assert.assertTrue(shareInfo.getUserSet().contains(testUser2));

    // isSharedWithUser should return true for path and file
    Assert.assertTrue(fileShareService.isSharedWithUser(rTestUser2, tSys, pathToShare, testUser2));
    Assert.assertTrue(fileShareService.isSharedWithUser(rTestUser2, tSys, fileToCheck, testUser2));

    // Check that testUser and testUser2 can now see the path
    fileOpsService.ls(rTestUser1, tSys, pathToShare, 1, 0, nullImpersonationId, sharedAppCtxFalse);
    fileOpsService.ls(rTestUser2, tSys, pathToShare, 1, 0, nullImpersonationId, sharedAppCtxFalse);
    fileOpsService.getFileInfo(rTestUser2, tSys, pathToShare, nullImpersonationId, sharedAppCtxFalse);
    fileOpsService.getFileInfo(rTestUser2, tSys, fileToCheck, nullImpersonationId, sharedAppCtxFalse);

    // Remove share.
    fileShareService.unSharePath(rTestUser1, sysId, pathToShare, userSet);
    // Get shareInfo. Should be empty.
    shareInfo = fileShareService.getShareInfo(rTestUser1, sysId, pathToShare);
    Assert.assertNotNull(shareInfo);
    Assert.assertNotNull(shareInfo.getUserSet());
    Assert.assertFalse(shareInfo.isPublic());
    Assert.assertTrue(shareInfo.getUserSet().isEmpty());

    // isSharedWithUser should return false
    Assert.assertFalse(fileShareService.isSharedWithUser(rTestUser2, tSys, pathToShare, testUser2));
    Assert.assertFalse(fileShareService.isSharedWithUser(rTestUser2, tSys, fileToCheck, testUser2));

    // Check that once again testUser can see the file and testUser2 cannot
    fileOpsService.ls(rTestUser1, tSys, pathToShare, 1, 0, nullImpersonationId, sharedAppCtxFalse);
    pass = false;
    try
    {
      fileOpsService.ls(rTestUser2, tSys, pathToShare, 1, 0, nullImpersonationId, sharedAppCtxFalse);
    }
    catch (ForbiddenException e) { pass = true; }
    Assert.assertTrue(pass, "User testUser2 should not be able to list path");
  }
}
