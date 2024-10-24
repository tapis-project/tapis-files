package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.files.api.BaseResourceConfig;
import edu.utexas.tacc.tapis.files.api.providers.FilePermissionsAuthz;
import edu.utexas.tacc.tapis.files.integration.transfers.IntegrationTestUtils;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCacheNoAuth;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService;
import edu.utexas.tacc.tapis.files.lib.services.FilePermsService;
import edu.utexas.tacc.tapis.files.lib.services.FileShareService;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.shared.ssh.SshSessionPool;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHConnection;
import edu.utexas.tacc.tapis.sharedapi.jaxrs.filters.JWTValidateRequestFilter;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.sharedapi.responses.TapisResponse;
import edu.utexas.tacc.tapis.systems.client.gen.model.AuthnEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.spi.Container;
import org.glassfish.jersey.server.spi.ContainerLifecycleListener;
import org.glassfish.jersey.test.TestProperties;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import javax.inject.Singleton;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static edu.utexas.tacc.tapis.files.integration.transfers.IntegrationTestUtils.getJwtForUser;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@Test(groups = {"integration"})
public class TestContentsRoutes extends BaseDatabaseIntegrationTest
{
  // Logger for tests
  private final Logger log = LoggerFactory.getLogger(TestContentsRoutes.class);

  private final static String oboTenant = "oboTenant";
  private final static String oboUser = "oboUser";

  private static final String TENANT = "dev";
  private static final String ROOT_DIR = "/data/home/testuser";
  private static final String SYSTEM_ID = "testSystem";
  private static final String TEST_USR = "testuser";
  private static final String TEST_USR1 = "testuser1";
  private static final String TEST_FILE1 = "testfile1.txt";
  private static final String TEST_FILE2 = "testfile2.txt";
  private static final String TEST_FILE3 = "testfile3.txt";
  private static final String TEST_FILE4 = "testfile4.txt";
  private static final int TEST_FILE_SIZE = 10 * 1024;
  private static final String CONTENT_ROUTE = "/v3/files/content";
  private static final String OPS_ROUTE = "/v3/files/ops";

  // Responses used in tests
  private static class FileStringResponse extends TapisResponse<String> {}

//  // TapisThreadContext that can be validated
//  TapisThreadContext threadContext = new TapisThreadContext();

  // Tapis Systems used in test.
  private final TapisSystem testSystemSSH;
  private final TapisSystem testSystemS3;
  private final TapisSystem testSystemDisabled;
  private final TapisSystem testSystemIrods;
  private final List<TapisSystem> testSystems = new ArrayList<>();
  private final List<TapisSystem> testSystemsNoS3 = new ArrayList<>();

  // mocking out the services
  private ServiceClients serviceClients;
  private SKClient skClient;
  private SystemsCache systemsCache;
  private SystemsCacheNoAuth systemsCacheNoAuth;
  private RemoteDataClientFactory remoteDataClientFactory = null;
  private final FilePermsService permsService = Mockito.mock(FilePermsService.class);

  /**
   * Private constructor
   */
  private TestContentsRoutes()
  {
    // Create Tapis Systems used in test.
    Credential creds;
    //SSH system with username+password
    creds = new Credential();
    creds.setAccessKey(TEST_USR);
    creds.setPassword("password");
    testSystemSSH = new TapisSystem();
    testSystemSSH.setId("testSystemSSH");
    testSystemSSH.setSystemType(SystemTypeEnum.LINUX);
    testSystemSSH.setHost("localhost");
    testSystemSSH.setPort(2222);
    testSystemSSH.setRootDir("/data/home/testuser/");
    testSystemSSH.setEffectiveUserId(TEST_USR);
    testSystemSSH.setDefaultAuthnMethod(AuthnEnum.PASSWORD);
    testSystemSSH.setAuthnCredential(creds);
    // S3 system with access key+secret
    creds = new Credential();
    creds.setAccessKey("user");
    creds.setAccessSecret("password");
    testSystemS3 = new TapisSystem();
    testSystemS3.setId("testSystemS3");
    testSystemS3.setSystemType(SystemTypeEnum.S3);
    testSystemS3.setOwner(TEST_USR1);
    testSystemS3.setHost("http://localhost");
    testSystemS3.setPort(9000);
    testSystemS3.setBucketName("test");
    testSystemS3.setRootDir("");
    testSystemS3.setDefaultAuthnMethod(AuthnEnum.ACCESS_KEY);
    testSystemS3.setAuthnCredential(creds);
    // Disabled system
    testSystemDisabled = new TapisSystem();
    testSystemDisabled.setId("testSystemDisabled");
    testSystemDisabled.setSystemType(SystemTypeEnum.S3);
    testSystemDisabled.setHost("http://localhost");
    testSystemDisabled.setPort(9000);
    testSystemDisabled.setBucketName("test");
    testSystemDisabled.setAuthnCredential(creds);
    testSystemDisabled.setRootDir("");
    testSystemDisabled.setEnabled(false);
    testSystemDisabled.setDefaultAuthnMethod(AuthnEnum.ACCESS_KEY);

    // IRODS system
    creds = new Credential();
    creds.setLoginUser("dev");
    creds.setPassword("dev");
    testSystemIrods = new TapisSystem();
    testSystemIrods.setId("testSystemIrods");
    testSystemIrods.setSystemType(SystemTypeEnum.IRODS);
    testSystemIrods.setHost("localhost");
    testSystemIrods.setPort(1247);
    testSystemIrods.setRootDir("/tempZone/home/dev/");
    testSystemIrods.setDefaultAuthnMethod(AuthnEnum.PASSWORD);
    testSystemIrods.setAuthnCredential(creds);
    testSystemIrods.setEffectiveUserId(creds.getLoginUser());

    // Collect all systems, for cleanup
    testSystems.add(testSystemSSH);
    testSystems.add(testSystemS3);
    testSystems.add(testSystemIrods);
    testSystems.add(testSystemDisabled);
    testSystemsNoS3.add(testSystemSSH);
    testSystemsNoS3.add(testSystemIrods);

    // NOTE: Part of an attemp to mock local thread context so we could check the context in ContentApiResource
    // But could not get it working, even using the static class/method support in recent versions of mockito.
//    // Initialize tapisThreadContext
//    threadContext.setJwtTenantId("dev");
//    threadContext.setJwtUser(TEST_USR1);
//    threadContext.setOboTenantId("dev");
//    threadContext.setOboUser(TEST_USR1);
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
    SSHConnection.setLocalNodeName("test");
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
                bind(serviceClients).to(ServiceClients.class);
                bind(tenantManager).to(TenantManager.class);
                bind(permsService).to(FilePermsService.class);
                bind(systemsCache).to(SystemsCache.class);
                bind(systemsCacheNoAuth).to(SystemsCacheNoAuth.class);
                bindAsContract(FileOpsService.class).in(Singleton.class);
                bindAsContract(FileShareService.class).in(Singleton.class);
                bindAsContract(FilePermsService.class);
                bind(serviceContext).to(ServiceContext.class);
                bindAsContract(RemoteDataClientFactory.class).in(Singleton.class);
              }
            });

    app.register(ContentApiResource.class);
    app.register(new ContainerLifecycleListener() {
      @Override
      public void onStartup(Container container) {
        remoteDataClientFactory =  container.getApplicationHandler().getInjectionManager().getInstance(RemoteDataClientFactory.class);
      }

      @Override
      public void onReload(Container container) {

      }

      @Override
      public void onShutdown(Container container) {

      }
    });
    FilePermsService.setSiteAdminTenantId("admin");
    FileShareService.setSiteAdminTenantId("admin");
    return app;
  }

  @BeforeClass
  @Override
  public void setUp() throws Exception
  {
    super.setUp();
    IntegrationTestUtils.clearSshSessionPoolInstance();
    SshSessionPool.init();
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
  }

  @BeforeMethod
  public void initMocks() throws Exception
  {
    Mockito.reset(skClient);
    Mockito.reset(serviceClients);
    Mockito.reset(systemsCache);
    Mockito.reset(systemsCacheNoAuth);
    when(serviceClients.getClient(any(String.class), any(String.class), eq(SKClient.class))).thenReturn(skClient);
    when(skClient.isPermitted(any(), any(), any())).thenReturn(true);
  }

  @BeforeMethod
  @AfterMethod
  // Remove all files from test systems
  public void cleanup() {
    testSystems.forEach((sys) -> {
      try {
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(TENANT, TEST_USR1, sys,
                IRemoteDataClientFactory.IMPERSONATION_ID_NULL, IRemoteDataClientFactory.SHARED_CTX_GRANTOR_NULL);
        when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(sys);
        when(systemsCacheNoAuth.getSystem(any(), any(), any())).thenReturn(sys);
        client.delete("/");
      } catch (NotFoundException ex) {
        // ignore not found exceptions - these are expected if there is nothing to remove.
      } catch (Exception ex) {
        log.error("Error: ", ex);
        throw new RuntimeException(ex);
      }
    });
  }

  /**
   * This is silly, but TestNG requires this to be an Object[]
   */
  @DataProvider(name="testSystemsProvider")
  public Object[] testSystemsProvider() { return new TapisSystem[]{testSystemS3, testSystemSSH, testSystemIrods}; }

  @DataProvider(name="testSystemsProviderNoS3")
  public Object[] testSystemsProviderNoS3() { return new TapisSystem[]{testSystemSSH, testSystemIrods}; }


  /* **************************************************************************** */
  /*                                Tests                                         */
  /* **************************************************************************** */

  @Test(dataProvider = "testSystemsProvider")
  public void testGetContents(TapisSystem testSystem) throws Exception
  {
    when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);
    when(systemsCacheNoAuth.getSystem(any(), any(), any())).thenReturn(testSystem);
    addTestFilesToSystem(testSystem, "testfile1.txt", 10 * 1024);
    Response response = target("/v3/files/content/" + testSystem.getId() + "/testfile1.txt")
            .request()
            .header("x-tapis-token", getJwtForUser("dev", TEST_USR1))
            .get();
    byte[] contents = response.readEntity(byte[].class);
    Assert.assertEquals(contents.length, 10 * 1024);
  }

  // For some reason this test almost always uses the S3 system causing it to fail.
  //  but oddly enough it will pass using dataProvider testSystemsProviderNoS3 and running it separately.
  // Even when using testSystemsProviderNoS3 it will fail (always uses S3) when all tests in class are run.
  // So it seems to be a test issue.
  // NOTE: Now it is working, not sure what was happening.
  @Test(dataProvider = "testSystemsProviderNoS3", enabled = true)
  public void testZipOutput(TapisSystem system) throws Exception {
    when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(system);
    when(systemsCacheNoAuth.getSystem(any(), any(), any())).thenReturn(system);
    addTestFilesToSystem(system, "a/test1.txt", 10 * 1000);
    addTestFilesToSystem(system, "a/b/test2.txt", 10 * 1000);
    addTestFilesToSystem(system, "a/b/test3.txt", 10 * 1000);

    Response response = target("/v3/files/content/" + system.getId() + "/a")
            .queryParam("zip", true)
            .request()
            .header("X-Tapis-Token", getJwtForUser("dev", TEST_USR1))
            .get();

    InputStream is = response.readEntity(InputStream.class);
    ZipInputStream zis = new ZipInputStream(is);
    ZipEntry ze;
    int count = 0;
    while ((ze = zis.getNextEntry()) != null)
    {
      log.info(system.getId() + ": " + ze.toString());
      count++;
    }
    // S3 does not do folders
    // S3 should have 3 entries and others should have 4 (3 files + 1 dir)
    if (system.getSystemType() == SystemTypeEnum.S3) Assert.assertEquals(count, 3);
    else Assert.assertEquals(count, 4);
  }

  @Ignore
  @Test(dataProvider = "testSystemsProvider", groups = {"slow"})
  public void testStreamLargeFile(TapisSystem system) throws Exception
  {
    when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(system);
    when(systemsCacheNoAuth.getSystem(any(), any(), any())).thenReturn(system);
    int filesize = 100 * 1000 * 1000;
    addTestFilesToSystem(system, "largetestfile1.txt", filesize);
    Response response = target("/v3/files/content/" + system.getId() + "/largetestfile1.txt")
            .request()
            .header("X-Tapis-Token", getJwtForUser("dev", TEST_USR1))
            .get();
    InputStream contents = response.readEntity(InputStream.class);
    Instant start = Instant.now();
    long fsize = 0;
    long chunk = 0;
    byte[] buffer = new byte[1024];
    while ((chunk = contents.read(buffer)) != -1) {
      fsize += chunk;
    }
    Duration time = Duration.between(start, Instant.now());
    log.info("file size={} MB: Download took {} ms: throughput={} MB/s", filesize / 1E6, time.toMillis(), filesize / (1E6 * time.toMillis() / 1000));
    Assert.assertEquals(fsize, filesize);
    contents.close();
  }

  @Test(dataProvider = "testSystemsProvider")
  public void testNotFound(TapisSystem system) throws Exception
  {
    when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(system);
    when(systemsCacheNoAuth.getSystem(any(), any(), any())).thenReturn(system);
    Response response = target("/v3/files/content/" + system.getId() + "/NOT-THERE.txt")
            .request()
            .header("X-Tapis-Token", getJwtForUser("dev", TEST_USR1))
            .get();
    Assert.assertEquals(response.getStatus(), 404);
  }

  @Test(dataProvider = "testSystemsProvider")
  public void testGetWithRange(TapisSystem system) throws Exception
  {
    when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(system);
    when(systemsCacheNoAuth.getSystem(any(), any(), any())).thenReturn(system);
    addTestFilesToSystem(system, "words.txt", 10 * 1024);
    Response response = target("/v3/files/content/" + system.getId() + "/words.txt")
            .request()
            .header("range", "0,1000")
            .header("X-Tapis-Token", getJwtForUser("dev", TEST_USR1))
            .get();
    Assert.assertEquals(response.getStatus(), 200);
    byte[] contents = response.readEntity(byte[].class);
    Assert.assertEquals(contents.length, 1000);
  }

  @Test(dataProvider = "testSystemsProvider")
  public void testGetWithMore(TapisSystem system) throws Exception
  {
    when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(system);
    when(systemsCacheNoAuth.getSystem(any(), any(), any())).thenReturn(system);
    addTestFilesToSystem(system, "words.txt", 10 * 1024);
    Response response = target("/v3/files/content/" + system.getId() + "/words.txt")
            .request()
            .header("more", "1")
            .header("X-Tapis-Token", getJwtForUser("dev", TEST_USR1))
            .get();
    Assert.assertEquals(response.getStatus(), 200);
    String contents = response.readEntity(String.class);
    Assert.assertEquals(response.getHeaders().getFirst("content-disposition"), "inline");
    //TODO: Its hard to say how many chars should be in there, some UTF-8 chars are 1 byte, some are 4. Need to have a better fixture
    Assert.assertTrue(contents.length() > 0);
  }

  @Test(dataProvider = "testSystemsProvider")
  public void testGetContentsHeaders(TapisSystem system) throws Exception
  {
    when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(system);
    when(systemsCacheNoAuth.getSystem(any(), any(), any())).thenReturn(system);
    // make sure content-type is application/octet-stream and filename is correct
    addTestFilesToSystem(system, "testfile1.txt", 10 * 1024);

    Response response = target("/v3/files/content/" + system.getId() + "/testfile1.txt")
            .request()
            .header("X-Tapis-Token", getJwtForUser("dev", TEST_USR1))
            .get();
    MultivaluedMap<String, Object> headers = response.getHeaders();
    String contentDisposition = (String) headers.getFirst("content-disposition");
    Assert.assertEquals(contentDisposition, "attachment; filename=testfile1.txt");
  }

  // Various requests that should result in a BadRequest status code (400)
  //  - Attempt to serve a folder from an S3 system, which is not allowed resulting in 400
  //  - Attempt to retrieve from a system which is disabled
  @Test
  public void testBadRequests() throws Exception
  {
//    when(systemsClient.getSystemWithCredentials(eq("testSystemS3"))).thenReturn(testSystemS3);
//    when(systemsClient.getSystemWithCredentials(eq("testSystemDisabled"))).thenReturn(testSystemDisabled);
//    when(systemsClient.getSystemWithCredentials(eq("testSystemSSH"))).thenReturn(testSystemSSH);
//    when(systemsClient.getSystemWithCredentials(eq("testSystemNotExist"), any())).thenThrow(new NotFoundException("Sys not found: testSystemNotExist"));
    when(systemsCache.getSystem(any(), eq("testSystemS3"), any(), any(), any())).thenReturn(testSystemS3);
    when(systemsCacheNoAuth.getSystem(any(), eq("testSystemS3"), any())).thenReturn(testSystemS3);
    when(systemsCache.getSystem(any(), eq("testSystemSSH"), any(), any(), any())).thenReturn(testSystemSSH);
    when(systemsCacheNoAuth.getSystem(any(), eq("testSystemSSH"), any())).thenReturn(testSystemSSH);
    when(systemsCache.getSystem(any(), eq("testSystemDisabled"), any(), any(), any())).thenReturn(testSystemDisabled);
    when(systemsCacheNoAuth.getSystem(any(), eq("testSystemDisabled"), any())).thenReturn(testSystemDisabled);
    addTestFilesToSystem(testSystemSSH, "dir1/testfile1.txt", 1024);

    // Attempt to retrieve a folder without using zip
    Response response = target("/v3/files/content/testSystemSSH/dir1")
            .request()
            .header("X-Tapis-Token", getJwtForUser("dev", TEST_USR1))
            .get();
    Assert.assertEquals(response.getStatus(), 400);

    // Attempt to retrieve from a system which is disabled
    response = target("/v3/files/content/testSystemDisabled/testfile1.txt")
            .request()
            .header("X-Tapis-Token", getJwtForUser("dev", TEST_USR1))
            .get();
    Assert.assertEquals(response.getStatus(), 404);
  }

  /* **************************************************************************** */
  /*                                Private Methods                               */
  /* **************************************************************************** */

  private void addTestFilesToSystem(TapisSystem system, String fileName, int fileSize) throws Exception
  {
    IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(TENANT, TEST_USR1, system,
            IRemoteDataClientFactory.IMPERSONATION_ID_NULL, IRemoteDataClientFactory.SHARED_CTX_GRANTOR_NULL);
    InputStream f1 = makeFakeFile(fileSize);
    client.upload(fileName, f1);
  }

  private InputStream makeFakeFile(long size)
  {
    return new RandomInputStream(size);
  }

  private static class RandomInputStream extends InputStream
  {
    private final long length;
    private long count;
    private final Random random;

    public RandomInputStream(long l) {length = l; random = new Random(); }

    @Override
    public int read()
    {
      if (count >= length) return -1;
      count++;
      return random.nextInt();
    }
  }
}
