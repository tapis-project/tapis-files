package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.files.api.BaseResourceConfig;
import edu.utexas.tacc.tapis.files.api.models.MkdirRequest;
import edu.utexas.tacc.tapis.files.api.models.MoveCopyRequest;
import edu.utexas.tacc.tapis.files.api.providers.FilePermissionsAuthz;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCacheNoAuth;
import edu.utexas.tacc.tapis.files.lib.caches.SSHConnectionCache;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService.MoveCopyOperation;
import edu.utexas.tacc.tapis.files.lib.services.FilePermsService;
import edu.utexas.tacc.tapis.files.lib.services.FileShareService;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.sharedapi.jaxrs.filters.JWTValidateRequestFilter;
import edu.utexas.tacc.tapis.sharedapi.responses.TapisResponse;
import edu.utexas.tacc.tapis.systems.client.gen.model.AuthnEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.apache.commons.lang3.tuple.Pair;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.inject.Singleton;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@Test(groups = {"integration"})
public class TestOpsRoutes extends BaseDatabaseIntegrationTest
{
  // Logger for tests
  private final Logger log = LoggerFactory.getLogger(TestOpsRoutes.class);

  private static final String TENANT = "dev";
  private static final String ROOT_DIR = "/data/home/testuser";
  private static final String SYSTEM_ID = "testSystem";
  private static final String TEST_USR = "testuser";
  private static final String TEST_USR1 = "testuser1";
  private static final String TEST_USR2 = "testuser2";
  private static final String TEST_FILE1 = "testfile1.txt";
  private static final String TEST_FILE2 = "testfile2.txt";
  private static final String TEST_FILE3 = "testfile3.txt";
  private static final String TEST_FILE4 = "testfile4.txt";
  private static final int TEST_FILE_SIZE = 10 * 1024;
  private static final String OPS_ROUTE = "/v3/files/ops";

  // Responses used in tests
  private static class FileListResponse extends TapisResponse<List<FileInfo>> {}
  private static class FileStringResponse extends TapisResponse<String> {}

  private final List<TapisSystem> testSystems = new ArrayList<>();
  private final List<TapisSystem> testSystemsNoS3 = new ArrayList<>();
  private final List<TapisSystem> testSystemsSSH = new ArrayList<>();
  private final List<TapisSystem> testSystemsIRODS = new ArrayList<>();

  // mocking out the services
  private ServiceClients serviceClients;
  private SKClient skClient;
  private SystemsCache systemsCache;
  private SystemsCacheNoAuth systemsCacheNoAuth;
  private final FilePermsService permsService = Mockito.mock(FilePermsService.class);

  /**
   * Private constructor
   */
  private TestOpsRoutes()
  {
    // Create Tapis Systems used in test.
    Credential creds;
    //SSH system with username+password
    creds = new Credential();
    creds.setAccessKey("testuser");
    creds.setPassword("password");
    TapisSystem testSystemSSH = new TapisSystem();
    testSystemSSH.setId("testSystem");
    testSystemSSH.setSystemType(SystemTypeEnum.LINUX);
    testSystemSSH.setOwner("testuser1");
    testSystemSSH.setHost("localhost");
    testSystemSSH.setPort(2222);
    testSystemSSH.setRootDir("/data/home/testuser/");
    testSystemSSH.setEffectiveUserId("testuser");
    testSystemSSH.setDefaultAuthnMethod(AuthnEnum.PASSWORD);
    testSystemSSH.setAuthnCredential(creds);
    // S3 system with access key+secret
    creds = new Credential();
    creds.setAccessKey("user");
    creds.setAccessSecret("password");
    TapisSystem testSystemS3 = new TapisSystem();
    testSystemS3.setId("testSystem");
    testSystemS3.setSystemType(SystemTypeEnum.S3);
    testSystemS3.setOwner("testuser1");
    testSystemS3.setHost("http://localhost");
    testSystemS3.setPort(9000);
    testSystemS3.setBucketName("test");
    testSystemS3.setRootDir("");
    testSystemS3.setDefaultAuthnMethod(AuthnEnum.ACCESS_KEY);
    testSystemS3.setAuthnCredential(creds);
    // IRODS system
    creds = new Credential();
    creds.setAccessKey("dev");
    creds.setAccessSecret("dev");
    TapisSystem testSystemIrods = new TapisSystem();
    testSystemIrods.setId("testSystem");
    testSystemIrods.setSystemType(SystemTypeEnum.IRODS);
    testSystemIrods.setOwner("testuser1");
    testSystemIrods.setHost("localhost");
    testSystemIrods.setPort(1247);
    testSystemIrods.setRootDir("/tempZone/home/dev/");
    testSystemIrods.setDefaultAuthnMethod(AuthnEnum.PASSWORD);
    testSystemIrods.setAuthnCredential(creds);

    // Collect all systems, for cleanup
    testSystems.add(testSystemSSH);
    testSystems.add(testSystemS3);
    testSystems.add(testSystemIrods);
    testSystemsNoS3.add(testSystemSSH);
    testSystemsNoS3.add(testSystemIrods);
    testSystemsSSH.add(testSystemSSH);
    testSystemsIRODS.add(testSystemIrods);
  }

  @Override
  protected ResourceConfig configure()
  {
    enable(TestProperties.LOG_TRAFFIC);
    enable(TestProperties.DUMP_ENTITY);
    forceSet(TestProperties.CONTAINER_PORT, "0");
    skClient = Mockito.mock(SKClient.class);
    serviceClients = Mockito.mock(ServiceClients.class);
    systemsCache = Mockito.mock(SystemsCache.class);
    systemsCacheNoAuth = Mockito.mock(SystemsCacheNoAuth.class);
    JWTValidateRequestFilter.setSiteId("tacc");
    JWTValidateRequestFilter.setService("files");
    ServiceContext serviceContext = Mockito.mock(ServiceContext.class);
    IRuntimeConfig runtimeConfig = RuntimeSettings.get();
    TenantManager tenantManager = TenantManager.getInstance(runtimeConfig.getTenantsServiceURL());
    tenantManager.getTenants();

    ResourceConfig app = new BaseResourceConfig()
            .register(JWTValidateRequestFilter.class)
            .register(FilePermissionsAuthz.class)
            .register(new AbstractBinder() {
              @Override
              protected void configure() {
                bindAsContract(FileOpsService.class).in(Singleton.class);
                bindAsContract(FileShareService.class).in(Singleton.class);
                bind(new SSHConnectionCache(CACHE_TIMEOUT_MINUTES, TimeUnit.MINUTES)).to(SSHConnectionCache.class);
                bind(serviceClients).to(ServiceClients.class);
                bind(tenantManager).to(TenantManager.class);
                bind(permsService).to(FilePermsService.class);
                bind(systemsCache).to(SystemsCache.class);
                bind(systemsCacheNoAuth).to(SystemsCacheNoAuth.class);
                bindAsContract(RemoteDataClientFactory.class);
                bind(serviceContext).to(ServiceContext.class);
              }
            });
    app.register(OperationsApiResource.class);
    FilePermsService.setSiteAdminTenantId("admin");
    FileShareService.setSiteAdminTenantId("admin");
    return app;
  }

  @BeforeClass
  @Override
  public void setUp() throws Exception
  {
    super.setUp();
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
  }

  /**
   * This is silly, but TestNG requires this to be an Object[]
   */
  @DataProvider(name = "testSystemsProvider")
  public Object[] testSystemsProvider()
  {
    return testSystems.toArray();
  }

  // Needed for the test client to be able to use Mutlipart/form posts;
  @Override
  protected void configureClient(ClientConfig config)
  {
    config.register(MultiPartFeature.class);
  }

  @BeforeMethod
  public void initMocks() throws Exception
  {
    Mockito.reset(skClient);
    Mockito.reset(serviceClients);
    when(serviceClients.getClient(any(String.class), any(String.class), eq(SKClient.class))).thenReturn(skClient);
    when(skClient.isPermitted(any(), any(), any())).thenReturn(true);
  }

  @BeforeMethod
  @AfterMethod
  // Remove all files from test systems
  public void cleanup()
  {
    testSystems.forEach( (sys)->
    {
      try {
        when(skClient.isPermitted(any(), any(), any())).thenReturn(true);
        when(systemsCache.getSystem(any(), any(), any())).thenReturn(sys);
        when(systemsCacheNoAuth.getSystem(any(), any())).thenReturn(sys);
        target(String.format("%s/%s/", OPS_ROUTE, SYSTEM_ID))
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
                .delete(FileStringResponse.class);
      }
      catch (Exception ex) { log.error(ex.getMessage(), ex); }
    });
  }

  @DataProvider(name="testSystemsProviderNoS3")
  public Object[] testSystemsProviderNoS3()
  {
    return testSystemsNoS3.toArray();
  }

  @DataProvider(name="testSystemsProviderSSH")
  public Object[] testSystemsProviderSSH() { return testSystemsSSH.toArray(); }

  @DataProvider(name="testSystemsProviderIRODS")
  public Object[] testSystemsProviderIRODS() { return testSystemsIRODS.toArray(); }

  @Test(dataProvider = "testSystemsProvider")
  public void testGetList(TapisSystem testSystem) throws Exception
  {
    when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);
    when(systemsCacheNoAuth.getSystem(any(), any())).thenReturn(testSystem);
    // Create file
    addFileToSystem(testSystem, TEST_FILE1, TEST_FILE_SIZE);
    FileListResponse response = target("/v3/files/ops/testSystem/")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
            .get(FileListResponse.class);
    FileInfo file = response.getResult().get(0);
    Assert.assertEquals(file.getPath(), TEST_FILE1);
    Assert.assertEquals(file.getName(), TEST_FILE1);
    Assert.assertEquals(file.getSize(), TEST_FILE_SIZE);
    Assert.assertNotNull(file.getUrl());
  }

  @Test(dataProvider = "testSystemsProvider")
    public void testGetListWithObo(TapisSystem testSystem) throws Exception
  {
        when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);
        when(systemsCacheNoAuth.getSystem(any(), any())).thenReturn(testSystem);
        addFileToSystem(testSystem, TEST_FILE1, TEST_FILE_SIZE);
        FileListResponse response = target("/v3/files/ops/testSystem/")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getServiceJwt())
            .header("x-tapis-user", TEST_USR1)
            .header("x-tapis-tenant", TENANT)
            .get(FileListResponse.class);
        FileInfo file = response.getResult().get(0);
        Assert.assertEquals(file.getName(), TEST_FILE1);
        Assert.assertEquals(file.getSize(), TEST_FILE_SIZE);
    }

    @Test(dataProvider = "testSystemsProvider")
    public void testGetListWithLimitAndOffset(TapisSystem testSystem) throws Exception
    {
        when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);
        when(systemsCacheNoAuth.getSystem(any(), any())).thenReturn(testSystem);
        addFileToSystem(testSystem, TEST_FILE1, TEST_FILE_SIZE);
        addFileToSystem(testSystem, TEST_FILE2, TEST_FILE_SIZE);
        addFileToSystem(testSystem, TEST_FILE3, TEST_FILE_SIZE);
        addFileToSystem(testSystem, TEST_FILE4, TEST_FILE_SIZE);
        addFileToSystem(testSystem, "testfile5.txt", TEST_FILE_SIZE);
        addFileToSystem(testSystem, "testfile6.txt", TEST_FILE_SIZE);
        addFileToSystem(testSystem, "testfile7.txt", TEST_FILE_SIZE);
        addFileToSystem(testSystem, "testfile8.txt", TEST_FILE_SIZE);

        FileListResponse response = target("/v3/files/ops/testSystem/")
            .queryParam("limit", "2")
            .queryParam("offset", "2")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
            .get(FileListResponse.class);
        List<FileInfo> listing = response.getResult();
        Assert.assertEquals(listing.size(), 2);
        Assert.assertEquals(listing.get(0).getName(), TEST_FILE3);
    }

    @Test(dataProvider = "testSystemsProvider")
    public void testGetListNoAuthz(TapisSystem testSystem) throws Exception
    {
        when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);
        when(systemsCacheNoAuth.getSystem(any(), any())).thenReturn(testSystem);
        addFileToSystem(testSystem, TEST_FILE1, TEST_FILE_SIZE);
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(false);
        Response response = target("/v3/files/ops/testSystem/testfile1.txt")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR2))
            .get();
        Assert.assertEquals(response.getStatus(), 403);
    }

    @Test(dataProvider = "testSystemsProvider")
    public void testDelete(TapisSystem testSystem) throws Exception
    {
        when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);
        when(systemsCacheNoAuth.getSystem(any(), any())).thenReturn(testSystem);
        addFileToSystem(testSystem, TEST_FILE1, TEST_FILE_SIZE);
        addFileToSystem(testSystem, TEST_FILE2, TEST_FILE_SIZE);
        addFileToSystem(testSystem, "dir1/testfile3.txt", TEST_FILE_SIZE);

        FileStringResponse response = target("/v3/files/ops/testSystem/dir1/testfile3.txt")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
            .delete(FileStringResponse.class);

        assertThrowsNotFoundForTestUser1("dir1/testfile3.txt");
    }

    @Test(dataProvider = "testSystemsProvider")
    public void testCopyFile(TapisSystem testSystem) throws Exception
    {
        when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);
        when(systemsCacheNoAuth.getSystem(any(), any())).thenReturn(testSystem);
        addFileToSystem(testSystem, "sample1.txt", TEST_FILE_SIZE);
        MoveCopyRequest request = new MoveCopyRequest();
        request.setOperation(MoveCopyOperation.COPY);
        request.setNewPath("/filestest/sample1.txt");

        FileStringResponse response = target("/v3/files/ops/testSystem/sample1.txt")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
            .put(Entity.json(request), FileStringResponse.class);

        List<FileInfo> listing = doListing(testSystem.getId(), "/filestest/sample1.txt", getJwtForUser(TENANT, TEST_USR1));
        Assert.assertEquals(listing.size(), 1);
        Assert.assertEquals(listing.get(0).getPath(), "filestest/sample1.txt");
    }

  @Test(dataProvider = "testSystemsProvider")
  public void testCopyFile2(TapisSystem testSystem) throws Exception
  {
    when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);
    when(systemsCacheNoAuth.getSystem(any(), any())).thenReturn(testSystem);
    addFileToSystem(testSystem, "/dir1/sample1.txt", TEST_FILE_SIZE);
    MoveCopyRequest request = new MoveCopyRequest();
    request.setOperation(MoveCopyOperation.COPY);
    request.setNewPath("/dir2/sample2.txt");

    FileStringResponse response = target("/v3/files/ops/testSystem/dir1/sample1.txt")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
            .put(Entity.json(request), FileStringResponse.class);

    List<FileInfo> listing = doListing(testSystem.getId(), "/dir2/sample2.txt", getJwtForUser(TENANT, TEST_USR1));
    Assert.assertEquals(listing.size(), 1);
    Assert.assertEquals(listing.get(0).getPath(), "dir2/sample2.txt");
  }

  @Test(dataProvider = "testSystemsProviderNoS3")
  public void testCopyFileToDir(TapisSystem testSystem) throws Exception
  {
    when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);
    when(systemsCacheNoAuth.getSystem(any(), any())).thenReturn(testSystem);
    addFileToSystem(testSystem, "/dir1/sample1.txt", TEST_FILE_SIZE);
    addFileToSystem(testSystem, "/dir2/sample2.txt", TEST_FILE_SIZE);
    MoveCopyRequest request = new MoveCopyRequest();
    request.setOperation(MoveCopyOperation.COPY);
    request.setNewPath("/dir2");

    FileStringResponse response = target("/v3/files/ops/testSystem/dir1/sample1.txt")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
            .put(Entity.json(request), FileStringResponse.class);

    List<FileInfo> listing = doListing(testSystem.getId(), "/dir2/sample1.txt", getJwtForUser(TENANT, TEST_USR1));
    Assert.assertEquals(listing.size(), 1);
    Assert.assertEquals(listing.get(0).getPath(), "dir2/sample1.txt");
  }

   @Test(dataProvider = "testSystemsProvider")
    public void testCopyFileShould404(TapisSystem testSystem) throws Exception
   {
        when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);
        when(systemsCacheNoAuth.getSystem(any(), any())).thenReturn(testSystem);
        addFileToSystem(testSystem, "sample1.txt", TEST_FILE_SIZE);
        MoveCopyRequest request = new MoveCopyRequest();
        request.setOperation(MoveCopyOperation.COPY);
        request.setNewPath("/filestest/sample1.txt");

        Assert.assertThrows(NotFoundException.class, ()->target("/v3/files/ops/testSystem/NOT-THERE.txt")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
            .put(Entity.json(request), FileStringResponse.class) );
    }

  @Test(dataProvider = "testSystemsProvider")
  public void testMoveFile(TapisSystem testSystem) throws Exception
  {
    when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);
    when(systemsCacheNoAuth.getSystem(any(), any())).thenReturn(testSystem);
    addFileToSystem(testSystem, TEST_FILE1, TEST_FILE_SIZE);

    MoveCopyRequest request = new MoveCopyRequest();
    request.setOperation(MoveCopyOperation.MOVE);
    request.setNewPath(TEST_FILE2);
    FileStringResponse response = target("/v3/files/ops/testSystem/testfile1.txt")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
            .put(Entity.json(request), FileStringResponse.class);

    List<FileInfo> listing = doListing(testSystem.getId(), TEST_FILE2, getJwtForUser(TENANT, TEST_USR1));
    for (FileInfo fi : listing)
    {
      System.out.println("Found file:"+ fi.getName() + " at path: " + fi.getPath());
    }
    Assert.assertEquals(listing.size(), 1);
    Assert.assertEquals(listing.get(0).getPath(), TEST_FILE2);
  }

  @Test(dataProvider = "testSystemsProviderNoS3")
  public void testMoveFileToDir(TapisSystem testSystem) throws Exception
  {
    when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);
    when(systemsCacheNoAuth.getSystem(any(), any())).thenReturn(testSystem);
    addFileToSystem(testSystem, TEST_FILE1, TEST_FILE_SIZE);
    addFileToSystem(testSystem, "dir1/testfile2.txt", TEST_FILE_SIZE);

    MoveCopyRequest request = new MoveCopyRequest();
    request.setOperation(MoveCopyOperation.MOVE);
    request.setNewPath("dir1");

    FileStringResponse response = target("/v3/files/ops/testSystem/testfile1.txt")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
            .put(Entity.json(request), FileStringResponse.class);

    List<FileInfo> listing = doListing(testSystem.getId(), "dir1/testfile1.txt", getJwtForUser(TENANT, TEST_USR1));
    Assert.assertEquals(listing.size(), 1);
    Assert.assertEquals(listing.get(0).getPath(), "dir1/testfile1.txt");
  }

    @Test(dataProvider = "testSystemsProviderNoS3")
    public void testMoveManyObjects1(TapisSystem testSystem) throws Exception
    {
        when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);
        when(systemsCacheNoAuth.getSystem(any(), any())).thenReturn(testSystem);
        addFileToSystem(testSystem, "test1.txt", TEST_FILE_SIZE);
        addFileToSystem(testSystem, "test2.txt", TEST_FILE_SIZE);
        addFileToSystem(testSystem, "dir1/1.txt", TEST_FILE_SIZE);
        addFileToSystem(testSystem, "dir1/dir2/2.txt", TEST_FILE_SIZE);
        addFileToSystem(testSystem, "dir1/dir2/dir3/3.txt", TEST_FILE_SIZE);
        addFileToSystem(testSystem, "dir1/dir2/dir3/dir4.txt", TEST_FILE_SIZE);

        MoveCopyRequest request = new MoveCopyRequest();
        request.setOperation(MoveCopyOperation.MOVE);
        request.setNewPath("renamed");

        FileStringResponse response = target("/v3/files/ops/testSystem/dir1/")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
            .put(Entity.json(request), FileStringResponse.class);


        assertThrowsNotFoundForTestUser1("/dir1/1.txt");
        Assert.assertTrue(doListing(testSystem.getId(), "/renamed", getJwtForUser(TENANT, TEST_USR1)).size() > 0);
    }

    /**
     * Tests of objects deeper in the tree are moved properly.
     *
     * @throws Exception
     */
    @Test(dataProvider = "testSystemsProviderNoS3")
    public void testMoveManyObjects2(TapisSystem testSystem) throws Exception
    {
        when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);
        when(systemsCacheNoAuth.getSystem(any(), any())).thenReturn(testSystem);

        addFileToSystem(testSystem, "test1.txt", TEST_FILE_SIZE);
        addFileToSystem(testSystem, "test2.txt", TEST_FILE_SIZE);
        addFileToSystem(testSystem, "dir1/1.txt", TEST_FILE_SIZE);
        addFileToSystem(testSystem, "dir1/dir2/2.txt", TEST_FILE_SIZE);
        addFileToSystem(testSystem, "dir1/dir2/dir3/3.txt", TEST_FILE_SIZE);
        addFileToSystem(testSystem, "dir1/dir2/dir3/dir4.txt", TEST_FILE_SIZE);

        MoveCopyRequest request = new MoveCopyRequest();
        request.setOperation(MoveCopyOperation.MOVE);
        request.setNewPath("renamed");

        FileStringResponse response = target("/v3/files/ops/testSystem/dir1/dir2/")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
            .put(Entity.json(request), FileStringResponse.class);

        List<FileInfo> listing = doListing(testSystem.getId(), "dir1/1.txt", getJwtForUser(TENANT, TEST_USR1));
        Assert.assertEquals(listing.size(), 1);
        listing = doListing(testSystem.getId(), "dir1/1.txt", getJwtForUser(TENANT, TEST_USR1));
        Assert.assertEquals(listing.size(), 1);
        assertThrowsNotFoundForTestUser1("/dir1/dir2/");
    }

    @Test(dataProvider = "testSystemsProviderNoS3")
    public void testDeleteManyObjects(TapisSystem testSystem) throws Exception
    {
        when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);
        when(systemsCacheNoAuth.getSystem(any(), any())).thenReturn(testSystem);
        addFileToSystem(testSystem, "test1.txt", TEST_FILE_SIZE);
        addFileToSystem(testSystem, "test2.txt", TEST_FILE_SIZE);
        addFileToSystem(testSystem, "dir1/1.txt", TEST_FILE_SIZE);
        addFileToSystem(testSystem, "dir1/dir2/2.txt", TEST_FILE_SIZE);
        addFileToSystem(testSystem, "dir1/dir2/dir3/3.txt", TEST_FILE_SIZE);
        addFileToSystem(testSystem, "dir1/dir2/dir3/4.txt", TEST_FILE_SIZE);

        FileStringResponse response = target("/v3/files/ops/testSystem/dir1/dir2/")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
            .delete(FileStringResponse.class);

        List<FileInfo> listing = doListing(testSystem.getId(), "dir1/1.txt", getJwtForUser(TENANT, TEST_USR1));
        Assert.assertEquals(listing.size(), 1);
        listing = doListing(testSystem.getId(), "dir1", getJwtForUser(TENANT, TEST_USR1));
        Assert.assertEquals(listing.size(), 1);
        assertThrowsNotFoundForTestUser1("/dir1/dir2/");

    }

    @Test(dataProvider = "testSystemsProvider")
    public void testInsertFile(TapisSystem testSystem) throws Exception
    {
        when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);
        when(systemsCacheNoAuth.getSystem(any(), any())).thenReturn(testSystem);
        InputStream inputStream = makeFakeFile(TEST_FILE_SIZE);
        File tempFile = File.createTempFile("tempfile", null);
        tempFile.deleteOnExit();
        FileOutputStream fos = new FileOutputStream(tempFile);
        fos.write(inputStream.readAllBytes());
        fos.close();
        FileDataBodyPart filePart = new FileDataBodyPart("file", tempFile);
        FormDataMultiPart form = new FormDataMultiPart();
        FormDataMultiPart multiPart = (FormDataMultiPart) form.bodyPart(filePart);
        FileStringResponse response = target("/v3/files/ops/testSystem/test-inserted.txt")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
            .post(Entity.entity(multiPart, MediaType.MULTIPART_FORM_DATA_TYPE), FileStringResponse.class);

        Assert.assertEquals(response.getStatus(), "success");

        FileListResponse listing = target("/v3/files/ops/testSystem/test-inserted.txt")
            .request()
            .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
            .get(FileListResponse.class);
        Assert.assertEquals(listing.getResult().size(), 1);
        Assert.assertEquals(listing.getResult().get(0).getName(), "test-inserted.txt");
    }

    @Test(dataProvider = "testSystemsProviderNoS3")
    public void testMkdir(TapisSystem testSystem) throws Exception
    {
        when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);
        when(systemsCacheNoAuth.getSystem(any(), any())).thenReturn(testSystem);

        MkdirRequest req = new MkdirRequest();
        req.setPath("newDirectory");
        
        FileStringResponse response = target("/v3/files/ops/testSystem/")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
            .post(Entity.json(req), FileStringResponse.class);

        FileListResponse listing = target("/v3/files/ops/testSystem/")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
            .get(FileListResponse.class);

        Assert.assertEquals(listing.getResult().size(), 1);
        Assert.assertEquals(listing.getResult().get(0).getName(), "newDirectory");
        Assert.assertEquals(listing.getResult().get(0).getType(), FileInfo.FILETYPE_DIR);
    }

    @Test(dataProvider = "testSystemsProviderNoS3")
    public void testMkdirNoSlash(TapisSystem testSystem) throws Exception
    {
        when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);
        when(systemsCacheNoAuth.getSystem(any(), any())).thenReturn(testSystem);
        MkdirRequest req = new MkdirRequest();
        req.setPath("newDirectory");

        FileStringResponse response = target("/v3/files/ops/testSystem/")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
            .post(Entity.json(req), FileStringResponse.class);

        FileListResponse listing = target("/v3/files/ops/testSystem/")
            .request()
            .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
            .get(FileListResponse.class);

        Assert.assertEquals(listing.getResult().size(), 1);
        Assert.assertEquals(listing.getResult().get(0).getName(), "newDirectory");
        Assert.assertEquals(listing.getResult().get(0).getType(), FileInfo.FILETYPE_DIR);
    }

  @DataProvider(name="mkdirDataProviderNoS3")
  public Object[] mkdirDataProviderNoS3()
  {
    List<String> directories = Arrays.asList(mkdirData());
    List<Pair<String, TapisSystem>> out = testSystemsNoS3.stream().flatMap(sys -> directories.stream().map(j -> Pair.of(j, sys)))
            .collect(Collectors.toList());
    return out.toArray();
  }

  @Test(dataProvider = "mkdirDataProviderNoS3")
    public void testMkdirWithSlashes(Pair<String, TapisSystem> inputs) throws Exception
  {
        when(systemsCache.getSystem(any(), any(), any())).thenReturn(inputs.getRight());
        when(systemsCacheNoAuth.getSystem(any(), any())).thenReturn(inputs.getRight());

        MkdirRequest req = new MkdirRequest();
        req.setPath(inputs.getLeft());

        FileStringResponse response = target("/v3/files/ops/testSystem/")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
            .post(Entity.json(req), FileStringResponse.class);

        FileListResponse listing = target("/v3/files/ops/testSystem/newDirectory/")
            .request()
            .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
            .get(FileListResponse.class);
        Assert.assertEquals(listing.getResult().size(), 1);
    }

    @DataProvider(name = "mkdirBadDataProvider")
    public Object[] mkdirBadDataProvider() {
        String[] mkdirData =  new String[]{
            "/newDirectory//../test",
            "newDirectory/../../test/",
            "../newDirectory/test",
            "newDirectory/..test"
        };
        List<String> directories = Arrays.asList(mkdirData);
        List<Pair<String, TapisSystem>> out = testSystems.stream().flatMap(sys -> directories.stream().map(j -> Pair.of(j, sys)))
            .collect(Collectors.toList());
        return out.toArray();
    }

    /**
     * All the funky paths should get cleaned up
     * @param inputs
     * @throws Exception
     */
    @Test(dataProvider = "mkdirBadDataProvider")
    public void testMkdirWithBadData(Pair<String, TapisSystem> inputs) throws Exception
    {
        when(systemsCache.getSystem(any(), any(), any())).thenReturn(inputs.getRight());
        when(systemsCacheNoAuth.getSystem(any(), any())).thenReturn(inputs.getRight());
        MkdirRequest req = new MkdirRequest();
        req.setPath(inputs.getLeft());
        Response response = target("/v3/files/ops/testSystem/")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
            .post(Entity.json(req), Response.class);

        Assert.assertEquals(response.getStatus(), 400);
    }

  /* **************************************************************************** */
  /*                                Private Methods                               */
  /* **************************************************************************** */

  private InputStream makeFakeFile(int size)
  {
    byte[] b = new byte[size];
    new Random().nextBytes(b);
    InputStream is = new ByteArrayInputStream(b);
    return is;
  }

  private void addFileToSystem(TapisSystem system, String fileName, int fileSize) throws Exception
  {
    InputStream inputStream = makeFakeFile(fileSize);
    File tempFile = File.createTempFile("tempfile", null);
    tempFile.deleteOnExit();
    FileOutputStream fos = new FileOutputStream(tempFile);
    fos.write(inputStream.readAllBytes());
    fos.close();
    FileDataBodyPart filePart = new FileDataBodyPart("file", tempFile);
    FormDataMultiPart form = new FormDataMultiPart();
    FormDataMultiPart multiPart = (FormDataMultiPart) form.bodyPart(filePart);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
    FileStringResponse response = target("/v3/files/ops/" + system.getId() + "/" + fileName)
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
            .post(Entity.entity(multiPart, MediaType.MULTIPART_FORM_DATA_TYPE), FileStringResponse.class);
  }

  private void assertThrowsNotFoundForTestUser1(String path)
  {
    Assert.assertThrows(NotFoundException.class, ()->{
      target("/v3/files/ops/testSystem/" + path)
              .request()
              .accept(MediaType.APPLICATION_JSON)
              .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
              .get(FileStringResponse.class);
    });
  }

  private List<FileInfo> doListing(String systemId, String path, String userJwt)
  {
    FileListResponse response = target("/v3/files/ops/" + systemId +"/" + path)
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", userJwt)
            .get(FileListResponse.class);
    return response.getResult();
  }

  private String[] mkdirData()
  {
    return new String[] { "//newDirectory///test", "newDirectory///test/", "newDirectory/test" };
  }
}
