package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.files.api.BaseResourceConfig;
import edu.utexas.tacc.tapis.files.api.models.MkdirRequest;
import edu.utexas.tacc.tapis.files.api.models.MoveCopyRequest;
import edu.utexas.tacc.tapis.files.api.providers.FilePermissionsAuthz;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
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
  // Responses used in tests
  private static class FileListResponse extends TapisResponse<List<FileInfo>> {}
  private static class FileStringResponse extends TapisResponse<String> {}

  private final List<TapisSystem> testSystems = new ArrayList<>();
  private final List<TapisSystem> testSystemsNoS3 = new ArrayList<>();
  private final List<TapisSystem> testSystemsIRODS = new ArrayList<>();

  // mocking out the services
  private ServiceClients serviceClients;
  private SKClient skClient;
  private SystemsCache systemsCache;
  private final FilePermsService permsService = Mockito.mock(FilePermsService.class);
  private static final String SITE_ID = "tacc";

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
    testSystemS3.setRootDir("/");
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
        target("/v3/files/ops/testSystem/")
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
                .delete(FileStringResponse.class);
      } catch (Exception ex) {
        log.error(ex.getMessage(), ex);
      }
    });
  }

  // Needed for the test client to be able to use Mutlipart/form posts;
  @Override
  protected void configureClient(ClientConfig config)
  {
    config.register(MultiPartFeature.class);
  }

  /**
   * This is silly, but TestNG requires this to be an Object[]
   */
  @DataProvider(name="testSystemsProvider")
  public Object[] testSystemsProvider()
  {
    return testSystems.toArray();
  }

  @DataProvider(name="testSystemsProviderNoS3")
  public Object[] testSystemsProviderNoS3()
  {
    return testSystemsNoS3.toArray();
  }

  @DataProvider(name="testSystemsProviderIRODS")
  public Object[] testSystemsProviderIRODS() { return testSystemsIRODS.toArray(); }

  @Test(dataProvider = "testSystemsProvider")
  public void testGetList(TapisSystem testSystem) throws Exception
  {
    when(systemsCache.getSystem(any(), any(), any())).thenReturn(testSystem);
    addTestFilesToSystem(testSystem, "testfile1.txt", 10 * 1024);
    FileListResponse response = target("/v3/files/ops/testSystem/")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .get(FileListResponse.class);
    FileInfo file = response.getResult().get(0);
    Assert.assertEquals(file.getPath(), "testfile1.txt");
    Assert.assertEquals(file.getName(), "testfile1.txt");
    Assert.assertEquals(file.getSize(), 10 * 1024);
    Assert.assertNotNull(file.getUrl());
  }

  @Test(dataProvider = "testSystemsProvider")
    public void testGetListWithObo(TapisSystem testSystem) throws Exception
  {
        when(systemsCache.getSystem(any(), any(), any())).thenReturn(testSystem);
        addTestFilesToSystem(testSystem, "testfile1.txt", 10 * 1024);
        FileListResponse response = target("/v3/files/ops/testSystem/")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getServiceJwt())
            .header("x-tapis-user", "testuser1")
            .header("x-tapis-tenant", "dev")
            .get(FileListResponse.class);
        FileInfo file = response.getResult().get(0);
        Assert.assertEquals(file.getName(), "testfile1.txt");
        Assert.assertEquals(file.getSize(), 10 * 1024);
    }

    @Test(dataProvider = "testSystemsProvider")
    public void testGetListWithLimitAndOffset(TapisSystem testSystem) throws Exception
    {
        when(systemsCache.getSystem(any(), any(), any())).thenReturn(testSystem);

        addTestFilesToSystem(testSystem, "testfile1.txt", 10 * 1024);
        addTestFilesToSystem(testSystem, "testfile2.txt", 10 * 1024);
        addTestFilesToSystem(testSystem, "testfile3.txt", 10 * 1024);
        addTestFilesToSystem(testSystem, "testfile4.txt", 10 * 1024);
        addTestFilesToSystem(testSystem, "testfile5.txt", 10 * 1024);
        addTestFilesToSystem(testSystem, "testfile6.txt", 10 * 1024);
        addTestFilesToSystem(testSystem, "testfile7.txt", 10 * 1024);
        addTestFilesToSystem(testSystem, "testfile8.txt", 10 * 1024);

        FileListResponse response = target("/v3/files/ops/testSystem/")
            .queryParam("limit", "2")
            .queryParam("offset", "2")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .get(FileListResponse.class);
        List<FileInfo> listing = response.getResult();
        Assert.assertEquals(listing.size(), 2);
        Assert.assertEquals(listing.get(0).getName(), "testfile3.txt");
    }

    @Test(dataProvider = "testSystemsProvider")
    public void testGetListNoAuthz(TapisSystem testSystem) throws Exception
    {
        when(systemsCache.getSystem(any(), any(), any())).thenReturn(testSystem);
        addTestFilesToSystem(testSystem, "testfile1.txt", 10 * 1024);
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(false);
        Response response = target("/v3/files/ops/testSystem/testfile1.txt")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .get();
        Assert.assertEquals(response.getStatus(), 403);
    }

    @Test(dataProvider = "testSystemsProvider")
    public void testDelete(TapisSystem testSystem) throws Exception
    {
        when(systemsCache.getSystem(any(), any(), any())).thenReturn(testSystem);
        addTestFilesToSystem(testSystem, "testfile1.txt", 10 * 1024);
        addTestFilesToSystem(testSystem, "testfile2.txt", 10 * 1024);
        addTestFilesToSystem(testSystem, "dir1/testfile3.txt", 10 * 1024);

        FileStringResponse response = target("/v3/files/ops/testSystem/dir1/testfile3.txt")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .delete(FileStringResponse.class);

        assertThrowsNotFoundForTestUser1("dir1/testfile3.txt");
    }

    @Test(dataProvider = "testSystemsProvider")
    public void testCopyFile(TapisSystem testSystem) throws Exception
    {
        when(systemsCache.getSystem(any(), any(), any())).thenReturn(testSystem);
        addTestFilesToSystem(testSystem, "sample1.txt", 10 * 1024);
        MoveCopyRequest request = new MoveCopyRequest();
        request.setOperation(MoveCopyOperation.COPY);
        request.setNewPath("/filestest/sample1.txt");

        FileStringResponse response = target("/v3/files/ops/testSystem/sample1.txt")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .put(Entity.json(request), FileStringResponse.class);

        List<FileInfo> listing = doListing(testSystem.getId(), "/filestest/sample1.txt", getJwtForUser("dev", "testuser1"));
        Assert.assertEquals(listing.size(), 1);
        Assert.assertEquals(listing.get(0).getPath(), "filestest/sample1.txt");
    }

  @Test(dataProvider = "testSystemsProvider")
  public void testCopyFile2(TapisSystem testSystem) throws Exception
  {
    when(systemsCache.getSystem(any(), any(), any())).thenReturn(testSystem);
    addTestFilesToSystem(testSystem, "/dir1/sample1.txt", 10 * 1024);
    MoveCopyRequest request = new MoveCopyRequest();
    request.setOperation(MoveCopyOperation.COPY);
    request.setNewPath("/dir2/sample2.txt");

    FileStringResponse response = target("/v3/files/ops/testSystem/dir1/sample1.txt")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .put(Entity.json(request), FileStringResponse.class);

    List<FileInfo> listing = doListing(testSystem.getId(), "/dir2/sample2.txt", getJwtForUser("dev", "testuser1"));
    Assert.assertEquals(listing.size(), 1);
    Assert.assertEquals(listing.get(0).getPath(), "dir2/sample2.txt");
  }

  @Test(dataProvider = "testSystemsProviderNoS3")
  public void testCopyFileToDir(TapisSystem testSystem) throws Exception
  {
    when(systemsCache.getSystem(any(), any(), any())).thenReturn(testSystem);
    addTestFilesToSystem(testSystem, "/dir1/sample1.txt", 10 * 1024);
    addTestFilesToSystem(testSystem, "/dir2/sample2.txt", 10 * 1024);
    MoveCopyRequest request = new MoveCopyRequest();
    request.setOperation(MoveCopyOperation.COPY);
    request.setNewPath("/dir2");

    FileStringResponse response = target("/v3/files/ops/testSystem/dir1/sample1.txt")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .put(Entity.json(request), FileStringResponse.class);

    List<FileInfo> listing = doListing(testSystem.getId(), "/dir2/sample1.txt", getJwtForUser("dev", "testuser1"));
    Assert.assertEquals(listing.size(), 1);
    Assert.assertEquals(listing.get(0).getPath(), "dir2/sample1.txt");
  }

   @Test(dataProvider = "testSystemsProvider")
    public void testCopyFileShould404(TapisSystem testSystem) throws Exception
   {
        when(systemsCache.getSystem(any(), any(), any())).thenReturn(testSystem);
        addTestFilesToSystem(testSystem, "sample1.txt", 10 * 1024);
        MoveCopyRequest request = new MoveCopyRequest();
        request.setOperation(MoveCopyOperation.COPY);
        request.setNewPath("/filestest/sample1.txt");

        Assert.assertThrows(NotFoundException.class, ()->target("/v3/files/ops/testSystem/NOT-THERE.txt")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .put(Entity.json(request), FileStringResponse.class) );
    }

  @Test(dataProvider = "testSystemsProvider")
  public void testMoveFile(TapisSystem testSystem) throws Exception
  {
    when(systemsCache.getSystem(any(), any(), any())).thenReturn(testSystem);
    addTestFilesToSystem(testSystem, "testfile1.txt", 10 * 1024);

    MoveCopyRequest request = new MoveCopyRequest();
    request.setOperation(MoveCopyOperation.MOVE);
    request.setNewPath("testfile2.txt");
    FileStringResponse response = target("/v3/files/ops/testSystem/testfile1.txt")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .put(Entity.json(request), FileStringResponse.class);

    List<FileInfo> listing = doListing(testSystem.getId(), "testfile2.txt", getJwtForUser("dev", "testuser1"));
    for (FileInfo fi : listing)
    {
      System.out.println("Found file:"+ fi.getName() + " at path: " + fi.getPath());
    }
    Assert.assertEquals(listing.size(), 1);
    Assert.assertEquals(listing.get(0).getPath(), "testfile2.txt");
  }

  @Test(dataProvider = "testSystemsProviderNoS3")
  public void testMoveFileToDir(TapisSystem testSystem) throws Exception
  {
    when(systemsCache.getSystem(any(), any(), any())).thenReturn(testSystem);
    addTestFilesToSystem(testSystem, "testfile1.txt", 10 * 1024);
    addTestFilesToSystem(testSystem, "dir1/testfile2.txt", 10 * 1024);

    MoveCopyRequest request = new MoveCopyRequest();
    request.setOperation(MoveCopyOperation.MOVE);
    request.setNewPath("dir1");

    FileStringResponse response = target("/v3/files/ops/testSystem/testfile1.txt")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .put(Entity.json(request), FileStringResponse.class);

    List<FileInfo> listing = doListing(testSystem.getId(), "dir1/testfile1.txt", getJwtForUser("dev", "testuser1"));
    Assert.assertEquals(listing.size(), 1);
    Assert.assertEquals(listing.get(0).getPath(), "dir1/testfile1.txt");
  }

    @Test(dataProvider = "testSystemsProviderNoS3")
    public void testMoveManyObjects1(TapisSystem testSystem) throws Exception
    {
        when(systemsCache.getSystem(any(), any(), any())).thenReturn(testSystem);
        addTestFilesToSystem(testSystem, "test1.txt", 10 * 1024);
        addTestFilesToSystem(testSystem, "test2.txt", 10 * 1024);
        addTestFilesToSystem(testSystem, "dir1/1.txt", 10 * 1024);
        addTestFilesToSystem(testSystem, "dir1/dir2/2.txt", 10 * 1024);
        addTestFilesToSystem(testSystem, "dir1/dir2/dir3/3.txt", 10 * 1024);
        addTestFilesToSystem(testSystem, "dir1/dir2/dir3/dir4.txt", 10 * 1024);

        MoveCopyRequest request = new MoveCopyRequest();
        request.setOperation(MoveCopyOperation.MOVE);
        request.setNewPath("renamed");

        FileStringResponse response = target("/v3/files/ops/testSystem/dir1/")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .put(Entity.json(request), FileStringResponse.class);


        assertThrowsNotFoundForTestUser1("/dir1/1.txt");
        Assert.assertTrue(doListing(testSystem.getId(), "/renamed", getJwtForUser("dev", "testuser1")).size() > 0);
    }

    /**
     * Tests of objects deeper in the tree are moved properly.
     *
     * @throws Exception
     */
    @Test(dataProvider = "testSystemsProviderNoS3")
    public void testMoveManyObjects2(TapisSystem testSystem) throws Exception
    {
        when(systemsCache.getSystem(any(), any(), any())).thenReturn(testSystem);

        addTestFilesToSystem(testSystem, "test1.txt", 10 * 1024);
        addTestFilesToSystem(testSystem, "test2.txt", 10 * 1024);
        addTestFilesToSystem(testSystem, "dir1/1.txt", 10 * 1024);
        addTestFilesToSystem(testSystem, "dir1/dir2/2.txt", 10 * 1024);
        addTestFilesToSystem(testSystem, "dir1/dir2/dir3/3.txt", 10 * 1024);
        addTestFilesToSystem(testSystem, "dir1/dir2/dir3/dir4.txt", 10 * 1024);

        MoveCopyRequest request = new MoveCopyRequest();
        request.setOperation(MoveCopyOperation.MOVE);
        request.setNewPath("renamed");

        FileStringResponse response = target("/v3/files/ops/testSystem/dir1/dir2/")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .put(Entity.json(request), FileStringResponse.class);

        List<FileInfo> listing = doListing(testSystem.getId(), "dir1/1.txt", getJwtForUser("dev", "testuser1"));
        Assert.assertEquals(listing.size(), 1);
        listing = doListing(testSystem.getId(), "dir1/1.txt", getJwtForUser("dev", "testuser1"));
        Assert.assertEquals(listing.size(), 1);
        assertThrowsNotFoundForTestUser1("/dir1/dir2/");
    }

    @Test(dataProvider = "testSystemsProviderNoS3")
    public void testDeleteManyObjects(TapisSystem testSystem) throws Exception
    {
        when(systemsCache.getSystem(any(), any(), any())).thenReturn(testSystem);
        addTestFilesToSystem(testSystem, "test1.txt", 10 * 1024);
        addTestFilesToSystem(testSystem, "test2.txt", 10 * 1024);
        addTestFilesToSystem(testSystem, "dir1/1.txt", 10 * 1024);
        addTestFilesToSystem(testSystem, "dir1/dir2/2.txt", 10 * 1024);
        addTestFilesToSystem(testSystem, "dir1/dir2/dir3/3.txt", 10 * 1024);
        addTestFilesToSystem(testSystem, "dir1/dir2/dir3/4.txt", 10 * 1024);

        FileStringResponse response = target("/v3/files/ops/testSystem/dir1/dir2/")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .delete(FileStringResponse.class);

        List<FileInfo> listing = doListing(testSystem.getId(), "dir1/1.txt", getJwtForUser("dev", "testuser1"));
        Assert.assertEquals(listing.size(), 1);
        listing = doListing(testSystem.getId(), "dir1", getJwtForUser("dev", "testuser1"));
        Assert.assertEquals(listing.size(), 1);
        assertThrowsNotFoundForTestUser1("/dir1/dir2/");

    }

    @Test(dataProvider = "testSystemsProvider")
    public void testInsertFile(TapisSystem testSystem) throws Exception
    {
        when(systemsCache.getSystem(any(), any(), any())).thenReturn(testSystem);
        InputStream inputStream = makeFakeFile(10 * 1024);
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
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .post(Entity.entity(multiPart, MediaType.MULTIPART_FORM_DATA_TYPE), FileStringResponse.class);

        Assert.assertEquals(response.getStatus(), "success");

        FileListResponse listing = target("/v3/files/ops/testSystem/test-inserted.txt")
            .request()
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .get(FileListResponse.class);
        Assert.assertEquals(listing.getResult().size(), 1);
        Assert.assertEquals(listing.getResult().get(0).getName(), "test-inserted.txt");
    }

    @Test(dataProvider = "testSystemsProviderNoS3")
    public void testMkdir(TapisSystem testSystem) throws Exception
    {
        when(systemsCache.getSystem(any(), any(), any())).thenReturn(testSystem);

        MkdirRequest req = new MkdirRequest();
        req.setPath("newDirectory");
        
        FileStringResponse response = target("/v3/files/ops/testSystem/")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .post(Entity.json(req), FileStringResponse.class);

        FileListResponse listing = target("/v3/files/ops/testSystem/")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .get(FileListResponse.class);

        Assert.assertEquals(listing.getResult().size(), 1);
        Assert.assertEquals(listing.getResult().get(0).getName(), "newDirectory");
        Assert.assertEquals(listing.getResult().get(0).getType(), FileInfo.FILETYPE_DIR);
    }

    @Test(dataProvider = "testSystemsProviderNoS3")
    public void testMkdirNoSlash(TapisSystem testSystem) throws Exception
    {
        when(systemsCache.getSystem(any(), any(), any())).thenReturn(testSystem);
        MkdirRequest req = new MkdirRequest();
        req.setPath("newDirectory");

        FileStringResponse response = target("/v3/files/ops/testSystem/")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .post(Entity.json(req), FileStringResponse.class);

        FileListResponse listing = target("/v3/files/ops/testSystem/")
            .request()
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
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

        MkdirRequest req = new MkdirRequest();
        req.setPath(inputs.getLeft());

        FileStringResponse response = target("/v3/files/ops/testSystem/")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .post(Entity.json(req), FileStringResponse.class);

        FileListResponse listing = target("/v3/files/ops/testSystem/newDirectory/")
            .request()
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
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
        MkdirRequest req = new MkdirRequest();
        req.setPath(inputs.getLeft());
        Response response = target("/v3/files/ops/testSystem/")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
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

  private void addTestFilesToSystem(TapisSystem system, String fileName, int fileSize) throws Exception
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
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .post(Entity.entity(multiPart, MediaType.MULTIPART_FORM_DATA_TYPE), FileStringResponse.class);
  }

  private void assertThrowsNotFoundForTestUser1(String path)
  {
    Assert.assertThrows(NotFoundException.class, ()->{
      target("/v3/files/ops/testSystem/" + path)
              .request()
              .accept(MediaType.APPLICATION_JSON)
              .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
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
