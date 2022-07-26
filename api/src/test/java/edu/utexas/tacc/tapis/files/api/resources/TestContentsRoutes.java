package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.files.api.BaseResourceConfig;
import edu.utexas.tacc.tapis.files.api.providers.FilePermissionsAuthz;
import edu.utexas.tacc.tapis.files.lib.caches.SSHConnectionCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService;
import edu.utexas.tacc.tapis.files.lib.services.FilePermsService;
import edu.utexas.tacc.tapis.files.lib.services.IFileOpsService;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHConnection;
import edu.utexas.tacc.tapis.sharedapi.jaxrs.filters.JWTValidateRequestFilter;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.sharedapi.responses.TapisResponse;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.AuthnEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.mockito.MockedStatic;
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
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

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
  private SystemsClient systemsClient;
  private SKClient skClient;
  private SystemsCache systemsCache;
  private final SSHConnectionCache sshConnectionCache = new SSHConnectionCache(CACHE_TIMEOUT_MINUTES, TimeUnit.MINUTES);
  private final RemoteDataClientFactory remoteDataClientFactory = new RemoteDataClientFactory(sshConnectionCache);
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
    creds.setAccessKey("testuser");
    creds.setPassword("password");
    testSystemSSH = new TapisSystem();
    testSystemSSH.setId("testSystemSSH");
    testSystemSSH.setSystemType(SystemTypeEnum.LINUX);
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
    testSystemS3 = new TapisSystem();
    testSystemS3.setId("testSystemS3");
    testSystemS3.setSystemType(SystemTypeEnum.S3);
    testSystemS3.setOwner("testuser1");
    testSystemS3.setHost("http://localhost");
    testSystemS3.setPort(9000);
    testSystemS3.setBucketName("test");
    testSystemS3.setRootDir("/");
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
    testSystemDisabled.setRootDir("/");
    testSystemDisabled.setEnabled(false);
    testSystemDisabled.setDefaultAuthnMethod(AuthnEnum.ACCESS_KEY);

    // IRODS system
    creds = new Credential();
    creds.setAccessKey("dev");
    creds.setAccessSecret("dev");
    testSystemIrods = new TapisSystem();
    testSystemIrods.setId("testSystemIrods");
    testSystemIrods.setSystemType(SystemTypeEnum.IRODS);
    testSystemIrods.setHost("localhost");
    testSystemIrods.setPort(1247);
    testSystemIrods.setRootDir("/tempZone/home/dev/");
    testSystemIrods.setDefaultAuthnMethod(AuthnEnum.PASSWORD);
    testSystemIrods.setAuthnCredential(creds);

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
//    threadContext.setJwtUser("testuser1");
//    threadContext.setOboTenantId("dev");
//    threadContext.setOboUser("testuser1");
  }

  @Override
  protected ResourceConfig configure()
  {
    enable(TestProperties.LOG_TRAFFIC);
    enable(TestProperties.DUMP_ENTITY);
    forceSet(TestProperties.CONTAINER_PORT, "0");
    skClient = Mockito.mock(SKClient.class);
    serviceClients = Mockito.mock(ServiceClients.class);
    systemsClient = Mockito.mock(SystemsClient.class);
    systemsCache = Mockito.mock(SystemsCache.class);
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
                bind(new SSHConnectionCache(CACHE_TIMEOUT_MINUTES, TimeUnit.MINUTES)).to(SSHConnectionCache.class);
                bind(serviceClients).to(ServiceClients.class);
                bind(tenantManager).to(TenantManager.class);
                bind(permsService).to(FilePermsService.class);
                bindAsContract(SystemsCache.class);
                bindAsContract(FilePermsService.class);
                bind(serviceContext).to(ServiceContext.class);
                bind(systemsCache).to(SystemsCache.class);
                bind(FileOpsService.class).to(IFileOpsService.class).in(Singleton.class);
                bind(remoteDataClientFactory).to(RemoteDataClientFactory.class);
                bind(sshConnectionCache).to(SSHConnectionCache.class);
              }
            });

    app.register(ContentApiResource.class);
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
    Mockito.reset(systemsCache);
    Mockito.reset(systemsClient);
    when(serviceClients.getClient(any(String.class), any(String.class), eq(SKClient.class))).thenReturn(skClient);
    when(serviceClients.getClient(any(String.class), any(String.class), eq(SystemsClient.class))).thenReturn(systemsClient);
    when(skClient.isPermitted(any(), any(), any())).thenReturn(true);
  }

  @BeforeMethod
  @AfterMethod
  // Remove all files from test systems
  public void cleanup()
  {
    testSystems.forEach( (sys)->
    {
      try
      {
        when(skClient.isPermitted(any(), any(), any())).thenReturn(true);
        when(systemsCache.getSystem(any(), any(), any())).thenReturn(sys);
        System.out.println("Cleanup for System: " + sys.getId());
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sys, "testuser");
        client.delete("/");
      }
      catch (Exception ex) { log.error(ex.getMessage(), ex); }    });
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
    when(systemsClient.getSystemWithCredentials(any(String.class))).thenReturn(testSystem);
    when(systemsCache.getSystem(any(), any(), any())).thenReturn(testSystem);
    addTestFilesToSystem(testSystem, "testfile1.txt", 10 * 1000);
    Response response = target("/v3/files/content/" + testSystem.getId() + "/testfile1.txt")
            .request()
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .get();
    byte[] contents = response.readEntity(byte[].class);
    Assert.assertEquals(contents.length, 10 * 1000);
  }

  // For some reason this test almost always uses the S3 system causing it to fail.
  //  but oddly enough it will pass using dataProvider testSystemsProviderNoS3 and running it separately.
  // Even when using testSystemsProviderNoS3 it will fail (always uses S3) when all tests in class are run.
  // So it seems to be a test issue.
  // NOTE: Now it is working, not sure what was happening.
  @Test(dataProvider = "testSystemsProviderNoS3", enabled = true)
  public void testZipOutput(TapisSystem system) throws Exception
  {
    when(systemsClient.getSystemWithCredentials(any(String.class))).thenReturn(system);
    addTestFilesToSystem(system, "a/test1.txt", 10 * 1000);
    addTestFilesToSystem(system, "a/b/test2.txt", 10 * 1000);
    addTestFilesToSystem(system, "a/b/test3.txt", 10 * 1000);

    Response response = target("/v3/files/content/" + system.getId() + "/a")
            .queryParam("zip", true)
            .request()
            .header("X-Tapis-Token", getJwtForUser("dev", "testuser1"))
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

  @Test(dataProvider = "testSystemsProvider")
  public void testStreamLargeFile(TapisSystem system) throws Exception
  {
    when(systemsClient.getSystemWithCredentials(any(String.class))).thenReturn(system);
    int filesize = 100 * 1000 * 1000;
    addTestFilesToSystem(system, "largetestfile1.txt", filesize);
    Response response = target("/v3/files/content/" + system.getId() + "/largetestfile1.txt")
            .request()
            .header("X-Tapis-Token", getJwtForUser("dev", "testuser1"))
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
    when(systemsClient.getSystemWithCredentials(any(String.class))).thenReturn(system);
    Response response = target("/v3/files/content/" + system.getId() + "/NOT-THERE.txt")
            .request()
            .header("X-Tapis-Token", getJwtForUser("dev", "testuser1"))
            .get();
    Assert.assertEquals(response.getStatus(), 404);
  }

  @Test(dataProvider = "testSystemsProvider")
  public void testGetWithRange(TapisSystem system) throws Exception
  {
    when(systemsClient.getSystemWithCredentials(any(String.class))).thenReturn(system);
    addTestFilesToSystem(system, "words.txt", 10 * 1024);
    Response response = target("/v3/files/content/" + system.getId() + "/words.txt")
            .request()
            .header("range", "0,1000")
            .header("X-Tapis-Token", getJwtForUser("dev", "testuser1"))
            .get();
    Assert.assertEquals(response.getStatus(), 200);
    byte[] contents = response.readEntity(byte[].class);
    Assert.assertEquals(contents.length, 1000);
  }

  @Test(dataProvider = "testSystemsProvider")
  public void testGetWithMore(TapisSystem system) throws Exception
  {
    when(systemsClient.getSystemWithCredentials(any(String.class))).thenReturn(system);
    addTestFilesToSystem(system, "words.txt", 10 * 1024);
    Response response = target("/v3/files/content/" + system.getId() + "/words.txt")
            .request()
            .header("more", "1")
            .header("X-Tapis-Token", getJwtForUser("dev", "testuser1"))
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
    when(systemsClient.getSystemWithCredentials(any(String.class))).thenReturn(system);
    // make sure content-type is application/octet-stream and filename is correct
    addTestFilesToSystem(system, "testfile1.txt", 10 * 1024);

    Response response = target("/v3/files/content/" + system.getId() + "/testfile1.txt")
            .request()
            .header("X-Tapis-Token", getJwtForUser("dev", "testuser1"))
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
    when(systemsClient.getSystemWithCredentials(eq("testSystemS3"))).thenReturn(testSystemS3);
    when(systemsClient.getSystemWithCredentials(eq("testSystemDisabled"))).thenReturn(testSystemDisabled);
    when(systemsClient.getSystemWithCredentials(eq("testSystemSSH"))).thenReturn(testSystemSSH);
//    when(systemsClient.getSystemWithCredentials(eq("testSystemNotExist"), any())).thenThrow(new NotFoundException("Sys not found: testSystemNotExist"));

    // Attempt to retrieve a folder
    Response response = target("/v3/files/content/testSystemS3/BAD-PATH/")
            .request()
            .header("X-Tapis-Token", getJwtForUser("dev", "testuser1"))
            .get();
    Assert.assertEquals(response.getStatus(), 400);

    // Attempt to retrieve from a system which is disabled
    response = target("/v3/files/content/testSystemDisabled/testfile1.txt")
            .request()
            .header("X-Tapis-Token", getJwtForUser("dev", "testuser1"))
            .get();
    Assert.assertEquals(response.getStatus(), 404);
  }

  /* **************************************************************************** */
  /*                                Private Methods                               */
  /* **************************************************************************** */

  private void addTestFilesToSystem(TapisSystem system, String fileName, int fileSize) throws Exception
  {
    IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, system, "testuser");
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
