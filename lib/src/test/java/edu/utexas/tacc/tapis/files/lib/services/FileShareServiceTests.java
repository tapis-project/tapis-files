package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.files.lib.Utils;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCacheNoAuth;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.ShareInfo;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.files.test.AbstractBinderBuilder;
import edu.utexas.tacc.tapis.files.test.TestUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.shared.ssh.SshSessionPool;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.hk2.api.ServiceLocator;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.ws.rs.ForbiddenException;
import java.io.InputStream;
import java.util.Collections;

import static org.mockito.Mockito.when;

@Test(groups = "integration")
public class FileShareServiceTests {
    private static final Logger log  = LoggerFactory.getLogger(FileShareServiceTests.class);
    private static final String JSON_TEST_PATH="edu/utexas/tacc/tapis/files/lib/clients/ShareTestsTestSystems.json";
    private static String devTenant = "dev";
    private final String testuser1 = "testuser1";
    private final String testuser2 = "testuser2";
    private final String testuser3 = "testuser3";

    private ResourceRequestUser rtestuser1;
    private ResourceRequestUser rtestuser2;
    private ResourceRequestUser rtestuser3;

    @BeforeSuite
    public void beforeSuite() {
        SshSessionPool.init();

        rtestuser1 = TestUtils.getRRUser(devTenant, testuser1);
        rtestuser2 = TestUtils.getRRUser(devTenant, testuser2);
        rtestuser3 = TestUtils.getRRUser(devTenant, testuser3);
    }

    @BeforeTest()
    public void setUp() throws Exception
    {
        Utils.clearSshSessionPoolInstance();
        SshSessionPool.init();

        TapisSystem testSystem = TestUtils.readSystem(JSON_TEST_PATH, "testsystem_ssh");

        ServiceLocator locator = new AbstractBinderBuilder()
                .mockSystemsCache(getSystemsCacheMock(devTenant, testSystem, testuser1, testuser2, testuser3))
                .mockSystemsCacheNoAuth(getSystesCacheNoAuthMock(devTenant, testSystem, testuser1, testuser2, testuser3))
                .buildAsServiceLocator();

        // for some reason this is required.  I don't really understand why.  If it's removed, the call to
        // permsService.grantPermissions will fail.
        ServiceContext serviceContext = locator.getService(ServiceContext.class);

        FilePermsService permsService = locator.getService(FilePermsService.class);

        permsService.grantPermission(devTenant, testuser1, testSystem.getId(), "/", FileInfo.Permission.MODIFY);

        RemoteDataClientFactory remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testuser1, testSystem,
                IRemoteDataClientFactory.IMPERSONATION_ID_NULL, IRemoteDataClientFactory.SHARED_CTX_GRANTOR_NULL);

        FileOpsService fileOpsService = locator.getService(FileOpsService.class);
        fileOpsService.delete(client, "/");
    }

    @AfterTest()
    public void tearDown() throws Exception
    {
        TapisSystem testSystem = TestUtils.readSystem(JSON_TEST_PATH, "testsystem_ssh");

        ServiceLocator locator = new AbstractBinderBuilder()
                .mockSystemsCache(getSystemsCacheMock(devTenant, testSystem, testuser1, testuser2, testuser3))
                .mockSystemsCacheNoAuth(getSystesCacheNoAuthMock(devTenant, testSystem, testuser1, testuser2, testuser3))
                .buildAsServiceLocator();

        FilePermsService permsService = locator.getService(FilePermsService.class);
        permsService.grantPermission(devTenant, testuser1, testSystem.getId(), "/", FileInfo.Permission.MODIFY);

        RemoteDataClientFactory remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testuser1, testSystem,
                IRemoteDataClientFactory.IMPERSONATION_ID_NULL, IRemoteDataClientFactory.SHARED_CTX_GRANTOR_NULL);
        FileOpsService fileOpsService = locator.getService(FileOpsService.class);
        fileOpsService.delete(client,"/");
    }

    // ===============================================
    // Test basic sharing of various paths
    // ===============================================
    @Test
    public void testSharePaths() throws Exception
    {
        TapisSystem testSystem = TestUtils.readSystem(JSON_TEST_PATH, "testsystem_ssh");

        ServiceLocator locator = new AbstractBinderBuilder()
                .mockSystemsCache(getSystemsCacheMock(devTenant, testSystem, testuser1, testuser2, testuser3))
                .mockSystemsCacheNoAuth(getSystesCacheNoAuthMock(devTenant, testSystem, testuser1, testuser2, testuser3))
                .buildAsServiceLocator();

        // Get the system
        TapisSystem tmpSys = LibUtils.getSystemIfEnabled(rtestuser1, locator.getService(SystemsCache.class), testSystem.getId());
        Assert.assertNotNull(tmpSys);
        testSharingOfPath(locator, tmpSys, "/", "test1.txt");
        testSharingOfPath(locator, tmpSys, "", "test1.txt");
        testSharingOfPath(locator, tmpSys, "/dir1/dir2/test1.txt", null);
    }

    // ===============================================
    // Test auth for sharing requests
    // ===============================================
    @Test
    public void testShareAuth() throws Exception
    {
        TapisSystem testSystem = TestUtils.readSystem(JSON_TEST_PATH, "testsystem_ssh");

        ServiceLocator locator = new AbstractBinderBuilder()
                .mockSystemsCache(getSystemsCacheMock(devTenant, testSystem, testuser1, testuser2, testuser3))
                .mockSystemsCacheNoAuth(getSystesCacheNoAuthMock(devTenant, testSystem, testuser1, testuser2, testuser3))
                .buildAsServiceLocator();

        // Get the system
        TapisSystem tmpSys = LibUtils.getSystemIfEnabled(rtestuser1, locator.getService(SystemsCache.class), testSystem.getId());
        Assert.assertNotNull(tmpSys);
        String sysId = tmpSys.getId();

        // Path to file used in testing
        String filePathStr = "/dir1/dir2/test2.txt";

        // Create set of users for sharing
        var userSet= Collections.singleton(testuser3);

        FilePermsService permsService = locator.getService(FilePermsService.class);
        FileOpsService fileOpsService = locator.getService(FileOpsService.class);
        FileShareService fileShareService = locator.getService(FileShareService.class);

        // Create file at path
        InputStream in = Utils.makeFakeFile(10*1024);
        permsService.grantPermission(devTenant, testuser1, sysId, filePathStr, FileInfo.Permission.MODIFY);
        fileOpsService.upload(rtestuser1, sysId, filePathStr, in);

        // Grant testUser1 full perms, testUser2 READ
        permsService.grantPermission(devTenant, testuser1, sysId, filePathStr, FileInfo.Permission.MODIFY);
        permsService.grantPermission(devTenant, testuser1, sysId, filePathStr, FileInfo.Permission.READ);
        permsService.grantPermission(devTenant, testuser2, sysId, filePathStr, FileInfo.Permission.READ);

        // Remove share from any previous failed run.
        fileShareService.removeAllSharesForPath(rtestuser1, sysId, filePathStr, true);
        // NOTE: If previous test failure resulted in share grants by user not system owner then
        //         it cannot be removed this way because SK uses oboUser to set grantor when removing a share.
        //         To clean up must remove perm check in FileShareService, add code here to remove shares
        //         using rTestUser2 or 3, make a run, then put back perm check in FileShareService.
        fileShareService.unSharePath(rtestuser1, sysId, filePathStr, userSet);

        // isSharedWithUser should return false
        Assert.assertFalse(fileShareService.isSharedWithUser(rtestuser1, tmpSys, filePathStr, testuser2));
        Assert.assertFalse(fileShareService.isSharedWithUser(rtestuser1, tmpSys, filePathStr, testuser3));

        // testUser2 has READ, so they should be able to get shareInfo
        fileShareService.getShareInfo(rtestuser2, sysId, filePathStr);

        // testUser3 does not have READ, so should not be able to get shareInfo
        boolean pass = false;
        try
        {
            fileShareService.getShareInfo(rtestuser3, sysId, filePathStr);
        }
        catch (ForbiddenException e) { pass = true; }
        Assert.assertTrue(pass, "User testUser3 should not be able to get share info");
        // testUser3 should not be able to create a share
        pass = false;
        try
        {
            fileShareService.sharePath(rtestuser3, sysId, filePathStr, userSet);
        }
        catch (ForbiddenException e) { pass = true; }
        Assert.assertTrue(pass, "User testUser3 should not be able to create share");

        // testUser2 is not system owner, so even with MODIFY should not be able to create a share.
        permsService.grantPermission(devTenant, testuser2, sysId, filePathStr, FileInfo.Permission.MODIFY);
        pass = false;
        try
        {
            fileShareService.sharePath(rtestuser2, sysId, filePathStr, userSet);
        }
        catch (ForbiddenException e) { pass = true; }
        Assert.assertTrue(pass, "User testUser2 should not be able to create share info");

        // Share with testUser3
        fileShareService.sharePath(rtestuser1, sysId, filePathStr, userSet);

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
        Assert.assertTrue(fileShareService.isSharedWithUser(rtestuser1, tmpSys, filePathStr, testuser3));

        // Remove share.
        fileShareService.unSharePath(rtestuser1, sysId, filePathStr, userSet);
        // Get shareInfo. Should be empty.
        ShareInfo shareInfo = fileShareService.getShareInfo(rtestuser1, sysId, filePathStr);
        Assert.assertNotNull(shareInfo);
        Assert.assertNotNull(shareInfo.getUserSet());
        Assert.assertFalse(shareInfo.isPublic());
        Assert.assertTrue(shareInfo.getUserSet().isEmpty());

        // isSharedWithUser should return false
        Assert.assertFalse(fileShareService.isSharedWithUser(rtestuser1, tmpSys, filePathStr, testuser2));
        Assert.assertFalse(fileShareService.isSharedWithUser(rtestuser1, tmpSys, filePathStr, testuser3));

        // Simple test of removeAllShares
        // Share with testUser3
        fileShareService.sharePath(rtestuser1, sysId, filePathStr, userSet);
        Assert.assertTrue(fileShareService.isSharedWithUser(rtestuser1, tmpSys, filePathStr, testuser3));
        fileShareService.removeAllSharesForPath(rtestuser1, sysId, filePathStr, false);
        Assert.assertFalse(fileShareService.isSharedWithUser(rtestuser1, tmpSys, filePathStr, testuser3));
    }


    // Test sharing of a path.
    // If fileToCreate is not blank then create the file.
    // If fileToCreate is blank then an attempt will be made to create a file using pathToShare
    private void testSharingOfPath(ServiceLocator locator, TapisSystem tSys, String pathToShare, String fileToCreate)
            throws TapisClientException
    {
        String sysId = tSys.getId();
        String fileToCheck;

        // Create file at path or using filetToCreate
        FileOpsService fileOpsService = locator.getService(FileOpsService.class);
        InputStream in = Utils.makeFakeFile(1024);
        if (StringUtils.isBlank(fileToCreate))
        {
            fileOpsService.upload(rtestuser1, sysId, pathToShare, in);
            fileToCheck = pathToShare;
        }
        else
        {
            fileOpsService.upload(rtestuser1, sysId, fileToCreate, in);
            fileToCheck = pathToShare = "/" + fileToCreate;
        }

        // Clean up any previous shares using recurse=true
        FileShareService fileShareService = locator.getService(FileShareService.class);
        fileShareService.removeAllSharesForPath(rtestuser1, sysId, pathToShare, true);

        // isSharedWithUser should return false
        Assert.assertFalse(fileShareService.isSharedWithUser(rtestuser2, tSys, pathToShare, testuser2));

        // Check that testUser can see the file and testUser2 cannot
        fileOpsService.ls(rtestuser1, sysId, pathToShare, new FileListingOpts.Builder().setPageSize(1).build(), null, null);
        fileOpsService.ls(rtestuser1, sysId, fileToCheck, new FileListingOpts.Builder().setPageSize(1).build(), null, null);
        fileOpsService.getFileInfo(rtestuser1, sysId, fileToCheck, true, null, null);
        boolean pass = false;
        try
        {
            fileOpsService.ls(rtestuser2, sysId, pathToShare, new FileListingOpts.Builder().setPageSize(1).build(), null, null);
        }
        catch (ForbiddenException e) { pass = true; }
        Assert.assertTrue(pass, "User testUser2 should not be able to list path");
        pass = false;
        try
        {
            fileOpsService.getFileInfo(rtestuser2, sysId, fileToCheck, true, null, null);
        }
        catch (ForbiddenException e) { pass = true; }
        Assert.assertTrue(pass, "User testUser2 should not be able to getFileInfo");
        pass = false;
        try
        {
            fileOpsService.ls(rtestuser2, sysId, fileToCheck, new FileListingOpts.Builder().setPageSize(1).build(), null, null);
        }
        catch (ForbiddenException e) { pass = true; }
        Assert.assertTrue(pass, "User testUser2 should not be able to list path to file");

        // Create set of users for sharing
        var userSet= Collections.singleton(testuser2);

        // Get shareInfo. Should be empty
        ShareInfo shareInfo = fileShareService.getShareInfo(rtestuser1, sysId, pathToShare);
        Assert.assertNotNull(shareInfo);
        Assert.assertNotNull(shareInfo.getUserSet());
        Assert.assertFalse(shareInfo.isPublic());
        Assert.assertTrue(shareInfo.getUserSet().isEmpty());

        //
        // Share path with testuser2
        //
        fileShareService.sharePath(rtestuser1, sysId, pathToShare, userSet);
        // Get shareInfo for file. Should be shared with 1 user, testuser2
        shareInfo = fileShareService.getShareInfo(rtestuser1, sysId, pathToShare);
        Assert.assertNotNull(shareInfo);
        Assert.assertNotNull(shareInfo.getUserSet());
        Assert.assertFalse(shareInfo.isPublic());
        Assert.assertFalse(shareInfo.getUserSet().isEmpty());
        Assert.assertEquals(shareInfo.getUserSet().size(), 1);
        Assert.assertTrue(shareInfo.getUserSet().contains(testuser2));

        // isSharedWithUser should return true for path and file
        Assert.assertTrue(fileShareService.isSharedWithUser(rtestuser2, tSys, pathToShare, testuser2));
        Assert.assertTrue(fileShareService.isSharedWithUser(rtestuser2, tSys, fileToCheck, testuser2));

        // Check that testUser and testUser2 can now see the path
        fileOpsService.ls(rtestuser1, sysId, pathToShare, new FileListingOpts.Builder().setPageSize(1).build(), null, null);
        fileOpsService.ls(rtestuser2, sysId, pathToShare, new FileListingOpts.Builder().setPageSize(1).build(), null, null);
        fileOpsService.getFileInfo(rtestuser2, sysId, pathToShare, true, null, null);
        fileOpsService.getFileInfo(rtestuser2, sysId, fileToCheck, true, null, null);

        // Remove share.
        fileShareService.unSharePath(rtestuser1, sysId, pathToShare, userSet);
        // Get shareInfo. Should be empty.
        shareInfo = fileShareService.getShareInfo(rtestuser1, sysId, pathToShare);
        Assert.assertNotNull(shareInfo);
        Assert.assertNotNull(shareInfo.getUserSet());
        Assert.assertFalse(shareInfo.isPublic());
        Assert.assertTrue(shareInfo.getUserSet().isEmpty());

        // isSharedWithUser should return false
        Assert.assertFalse(fileShareService.isSharedWithUser(rtestuser2, tSys, pathToShare, testuser2));
        Assert.assertFalse(fileShareService.isSharedWithUser(rtestuser2, tSys, fileToCheck, testuser2));

        // Check that once again testUser can see the file and testUser2 cannot
        fileOpsService.ls(rtestuser1, sysId, pathToShare, new FileListingOpts.Builder().setPageSize(1).build(), null, null);
        pass = false;
        try
        {
            fileOpsService.ls(rtestuser2, sysId, pathToShare, new FileListingOpts.Builder().setPageSize(1).build(), null, null);
        }
        catch (ForbiddenException e) { pass = true; }
        Assert.assertTrue(pass, "User testUser2 should not be able to list path");
    }

    private SystemsCache getSystemsCacheMock(String tenant, TapisSystem system, String ... users) throws ServiceException {
        SystemsCache systemsCacheMock = Mockito.mock(SystemsCache.class);
        for(String user : users) {
            when(systemsCacheMock.getSystem(tenant, system.getId(), user, null, null)).thenReturn(system);
            when(systemsCacheMock.getSystem(tenant, system.getId(), user)).thenReturn(system);
        }

        return systemsCacheMock;
    }

    private SystemsCacheNoAuth getSystesCacheNoAuthMock(String tenant, TapisSystem system, String ... users) throws ServiceException {
        SystemsCacheNoAuth systemsCacheNoAuthMock = Mockito.mock(SystemsCacheNoAuth.class);
        for(String user : users) {
            when(systemsCacheNoAuthMock.getSystem(tenant, system.getId(), user)).thenReturn(system);
        }

        return systemsCacheNoAuthMock;
    }



}
