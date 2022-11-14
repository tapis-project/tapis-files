package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.files.api.BaseResourceConfig;
import edu.utexas.tacc.tapis.files.api.models.NativeLinuxOpRequest;
import edu.utexas.tacc.tapis.files.api.providers.FilePermissionsAuthz;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCacheNoAuth;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.models.FileStatInfo;
import edu.utexas.tacc.tapis.files.lib.models.NativeLinuxOpResult;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService;
import edu.utexas.tacc.tapis.files.lib.services.FilePermsService;
import edu.utexas.tacc.tapis.files.lib.services.FileShareService;
import edu.utexas.tacc.tapis.files.lib.services.FileUtilsService;
import edu.utexas.tacc.tapis.files.lib.services.FileUtilsService.NativeLinuxOperation;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.files.lib.caches.SSHConnectionCache;
import edu.utexas.tacc.tapis.sharedapi.jaxrs.filters.JWTValidateRequestFilter;
import edu.utexas.tacc.tapis.sharedapi.responses.TapisResponse;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.AuthnEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.apache.commons.lang3.StringUtils;
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
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/*
 * Tests for getStatInfo and runLinuxNativeOp (chmod, chown, chgrp)
 */
@Test(groups = {"integration"})
public class TestLibUtilsRoutes extends BaseDatabaseIntegrationTest
{
  private final Logger log = LoggerFactory.getLogger(TestLibUtilsRoutes.class);

  private static class FileStatInfoResponse extends TapisResponse<FileStatInfo> { }
  private static class NativeLinuxOpResultResponse extends TapisResponse<NativeLinuxOpResult> { }
  private static class FileStringResponse extends TapisResponse<String> { }

  private final List<TapisSystem> testSystems = new ArrayList<>();

  private static final String TENANT = "dev";
  private static final String ROOT_DIR = "/data/home/testuser";
  private static final String SYSTEM_ID = "testSystem";
  private static final String TEST_USR = "testuser";
  private static final String TEST_USR_NEW = "testuser_new";
  private static final String TEST_GRP = "testuser";
  private static final String TEST_GRP_NEW = "testuser_new";
  private static final String TEST_USR1 = "testuser1";
  private static final String TEST_FILE1 = "testfile1.txt";
  private static final String TEST_FILE2 = "testfile2.txt";
  private static final String TEST_FILE3 = "testfile3.txt";
  private static final String TEST_FILE4 = "testfile4.txt";
  private static final String TEST_FILE_PERMS = "rw-r--r--";
  private static final String TEST_CHMOD_ARG = "755";
  private static final String TEST_FILE_NEWPERMS = "rwxr-xr-x";
  private static final int TEST_FILE_SIZE = 10 * 1024;
  private static final String OPS_ROUTE = "/v3/files/ops";
  private static final String UTILS_ROUTE = "/v3/files/utils/linux";

  // mocking out the services
  private ServiceClients serviceClients;
  private SystemsClient systemsClient;
  private SKClient skClient;
  private final FilePermsService permsService = Mockito.mock(FilePermsService.class);

  /*
   * Private constructor to create and setup systems used in tests
   */
  private TestLibUtilsRoutes()
  {
    //SSH system with username/password
    Credential sshCreds = new Credential();
    sshCreds.setPassword("password");

    TapisSystem testSystemSSH = new TapisSystem();
    testSystemSSH.setSystemType(SystemTypeEnum.LINUX);
    testSystemSSH.setAuthnCredential(sshCreds);
    testSystemSSH.setHost("localhost");
    testSystemSSH.setPort(2222);
    testSystemSSH.setRootDir(ROOT_DIR);
    testSystemSSH.setId(SYSTEM_ID);
    testSystemSSH.setEffectiveUserId(TEST_USR);
    testSystemSSH.setDefaultAuthnMethod(AuthnEnum.PASSWORD);

    testSystems.add(testSystemSSH);
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
    JWTValidateRequestFilter.setSiteId("tacc");
    JWTValidateRequestFilter.setService("files");
    ServiceContext serviceContext = Mockito.mock(ServiceContext.class);
    IRuntimeConfig runtimeConfig = RuntimeSettings.get();
    TenantManager tenantManager = TenantManager.getInstance(runtimeConfig.getTenantsServiceURL());
    tenantManager.getTenants();

    ResourceConfig app = new BaseResourceConfig()
            .register(JWTValidateRequestFilter.class)
            .register(FilePermissionsAuthz.class)
            .register(new AbstractBinder()
            {
              @Override
              protected void configure()
              {
                bind(new SSHConnectionCache(CACHE_TIMEOUT_MINUTES, TimeUnit.MINUTES)).to(SSHConnectionCache.class);
                bind(serviceClients).to(ServiceClients.class);
                bind(tenantManager).to(TenantManager.class);
                bind(permsService).to(FilePermsService.class);
                bindAsContract(SystemsCache.class).in(Singleton.class);
                bindAsContract(SystemsCacheNoAuth.class).in(Singleton.class);
                bindAsContract(FileOpsService.class).in(Singleton.class);
                bindAsContract(FileUtilsService.class).in(Singleton.class);
                bindAsContract(FileShareService.class).in(Singleton.class);
                bindAsContract(RemoteDataClientFactory.class);
                bind(serviceContext).to(ServiceContext.class);
              }
            });

    app.register(OperationsApiResource.class);
    app.register(UtilsLinuxApiResource.class);
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
    testSystems.forEach((sys) -> {
      try
      {
        when(skClient.isPermitted(any(), any(), any())).thenReturn(true);
        when(systemsClient.getSystemWithCredentials(any())).thenReturn(sys);
        target(String.format("%s/%s/", OPS_ROUTE, SYSTEM_ID))
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
                .delete(FileStringResponse.class);
      }
      catch (Exception ex) { log.error(ex.getMessage(), ex); }
    });
  }

  /*
   * Test getStatInfo
   */
  @Test(dataProvider = "testSystemsProvider")
  public void testGetStatInfo(TapisSystem testSystem) throws Exception
  {
    when(systemsClient.getSystemWithCredentials(any())).thenReturn(testSystem);
    // Create file
    addTestFilesToSystem(testSystem, TEST_FILE1, TEST_FILE_SIZE);
    // Get stat info and check file properties
    FileStatInfoResponse response = target(String.format("%s/%s/%s", UTILS_ROUTE, SYSTEM_ID, TEST_FILE1))
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getServiceJwt())
            .header("x-tapis-user", TEST_USR1)
            .header("x-tapis-tenant", TENANT)
            .get(FileStatInfoResponse.class);
    Assert.assertNotNull(response);
    FileStatInfo result = response.getResult();
    Assert.assertNotNull(result);
    System.out.printf("FileStatInfo result:%n%s%n", result);
    validateFileProperties(result, TEST_FILE1, TEST_FILE_PERMS);
  }

  /*
   * Test linux chmod operation
   */
  @Test(dataProvider = "testSystemsProvider")
  public void testLinuxChmod(TapisSystem testSystem) throws Exception
  {
    when(systemsClient.getSystemWithCredentials(any())).thenReturn(testSystem);
    // Create file
    addTestFilesToSystem(testSystem, TEST_FILE2, TEST_FILE_SIZE);
    // Get stat info and check file properties
    FileStatInfoResponse response1 = target(String.format("%s/%s/%s", UTILS_ROUTE, SYSTEM_ID, TEST_FILE2))
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getServiceJwt())
            .header("x-tapis-user", TEST_USR1)
            .header("x-tapis-tenant", TENANT)
            .get(FileStatInfoResponse.class);
    Assert.assertNotNull(response1);
    FileStatInfo fileStatInfo = response1.getResult();
    validateFileProperties(fileStatInfo, TEST_FILE2, TEST_FILE_PERMS);

    // Update permissions to 755 using chmod operation and check file properties
    var req = new NativeLinuxOpRequest();
    req.setOperation(NativeLinuxOperation.CHMOD);
    req.setArgument(TEST_CHMOD_ARG);
    NativeLinuxOpResultResponse response2 = target(String.format("%s/%s/%s", UTILS_ROUTE, SYSTEM_ID, TEST_FILE2))
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
            .post(Entity.json(req), NativeLinuxOpResultResponse.class);
    Assert.assertNotNull(response2);
    NativeLinuxOpResult result = response2.getResult();
    Assert.assertNotNull(result);
    System.out.printf("NativeLinuxOp result:%n%s%n", result);
    Assert.assertEquals(result.getExitCode(), 0);

    // Get stat info and check file properties after chmod
    response1 = target(String.format("%s/%s/%s", UTILS_ROUTE, SYSTEM_ID, TEST_FILE2))
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getServiceJwt())
            .header("x-tapis-user", TEST_USR1)
            .header("x-tapis-tenant", TENANT)
            .get(FileStatInfoResponse.class);
    Assert.assertNotNull(response1);
    fileStatInfo = response1.getResult();
    validateFileProperties(fileStatInfo, TEST_FILE2, TEST_FILE_NEWPERMS);
  }

  /*
   * Test linux chown operation.
   * Currently there is only one user defined on the test system.
   * Check that:
   *   - chown to same user runs without error
   *   - chown to undefined username returns with exitCode=1 and stdErr contains "invalid user"
   */
  @Test(dataProvider = "testSystemsProvider")
  public void testLinuxChown(TapisSystem testSystem) throws Exception
  {
    when(systemsClient.getSystemWithCredentials(any())).thenReturn(testSystem);
    // Create file
    addTestFilesToSystem(testSystem, TEST_FILE3, TEST_FILE_SIZE);
    // Get stat info and check file properties
    FileStatInfoResponse response1 = target(String.format("%s/%s/%s", UTILS_ROUTE, SYSTEM_ID, TEST_FILE3))
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getServiceJwt())
            .header("x-tapis-user", TEST_USR1)
            .header("x-tapis-tenant", TENANT)
            .get(FileStatInfoResponse.class);
    Assert.assertNotNull(response1);
    FileStatInfo fileStatInfo = response1.getResult();
    validateFileProperties(fileStatInfo, TEST_FILE3, TEST_FILE_PERMS);

    // Run chown testuser
    var req = new NativeLinuxOpRequest();
    req.setOperation(NativeLinuxOperation.CHOWN);
    req.setArgument(TEST_USR);
    NativeLinuxOpResultResponse response2 = target(String.format("%s/%s/%s", UTILS_ROUTE, SYSTEM_ID, TEST_FILE3))
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
            .post(Entity.json(req), NativeLinuxOpResultResponse.class);
    Assert.assertNotNull(response2);
    NativeLinuxOpResult result = response2.getResult();
    Assert.assertNotNull(result);
    System.out.printf("NativeLinuxOp result:%n%s%n", result);
    Assert.assertEquals(result.getExitCode(), 0);

    // Run chown testuser_new, should have exitCode=1
    req = new NativeLinuxOpRequest();
    req.setOperation(NativeLinuxOperation.CHOWN);
    req.setArgument(TEST_USR_NEW);
    response2 = target(String.format("%s/%s/%s", UTILS_ROUTE, SYSTEM_ID, TEST_FILE3))
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
            .post(Entity.json(req), NativeLinuxOpResultResponse.class);
    Assert.assertNotNull(response2);
    result = response2.getResult();
    Assert.assertNotNull(result);
    System.out.printf("NativeLinuxOp result:%n%s%n", result);
    Assert.assertEquals(result.getExitCode(), 1);
    Assert.assertFalse(StringUtils.isBlank(result.getStdErr()));
    Assert.assertTrue(result.getStdErr().contains("invalid user"));
  }

  /*
   * Test linux chgrp operation.
   * Currently there is only one user defined on the test system.
   * Check that:
   *   - chgrp to same group runs without error
   *   - chgrp to undefined group returns with exitCode=1 and stdErr contains "invalid user"
   */
  @Test(dataProvider = "testSystemsProvider")
  public void testLinuxChgrp(TapisSystem testSystem) throws Exception
  {
    when(systemsClient.getSystemWithCredentials(any())).thenReturn(testSystem);
    // Create file
    addTestFilesToSystem(testSystem, TEST_FILE4, TEST_FILE_SIZE);
    // Get stat info and check file properties
    FileStatInfoResponse response1 = target(String.format("%s/%s/%s", UTILS_ROUTE, SYSTEM_ID, TEST_FILE4))
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getServiceJwt())
            .header("x-tapis-user", TEST_USR1)
            .header("x-tapis-tenant", TENANT)
            .get(FileStatInfoResponse.class);
    Assert.assertNotNull(response1);
    FileStatInfo fileStatInfo = response1.getResult();
    validateFileProperties(fileStatInfo, TEST_FILE4, TEST_FILE_PERMS);

    // Run chown testuser
    var req = new NativeLinuxOpRequest();
    req.setOperation(NativeLinuxOperation.CHGRP);
    req.setArgument(TEST_GRP);
    NativeLinuxOpResultResponse response2 = target(String.format("%s/%s/%s", UTILS_ROUTE, SYSTEM_ID, TEST_FILE4))
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
            .post(Entity.json(req), NativeLinuxOpResultResponse.class);
    Assert.assertNotNull(response2);
    NativeLinuxOpResult result = response2.getResult();
    Assert.assertNotNull(result);
    System.out.printf("NativeLinuxOp result:%n%s%n", result);
    Assert.assertEquals(result.getExitCode(), 0);

    // Run chown testgrp_new, should have exitCode=1
    req = new NativeLinuxOpRequest();
    req.setOperation(NativeLinuxOperation.CHGRP);
    req.setArgument(TEST_GRP_NEW);
    response2 = target(String.format("%s/%s/%s", UTILS_ROUTE, SYSTEM_ID, TEST_FILE4))
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
            .post(Entity.json(req), NativeLinuxOpResultResponse.class);
    Assert.assertNotNull(response2);
    result = response2.getResult();
    Assert.assertNotNull(result);
    System.out.printf("NativeLinuxOp result:%n%s%n", result);
    Assert.assertEquals(result.getExitCode(), 1);
    Assert.assertFalse(StringUtils.isBlank(result.getStdErr()));
    Assert.assertTrue(result.getStdErr().contains("invalid group"));
  }

  // =======================================
  // ====== Private methods ================
  // =======================================

  private InputStream makeFakeFile(int size) {
    byte[] b = new byte[size];
    new Random().nextBytes(b);
    return new ByteArrayInputStream(b);
  }

  /*
   * Add file to a system using specified file name and file size.
   * File is added under the root directory.
   */
  private void addTestFilesToSystem(TapisSystem system, String fileName, int fileSize) throws Exception {
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
    FileStringResponse response = target(String.format("%s/%s/%s", OPS_ROUTE, system.getId(), fileName))
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
            .post(Entity.entity(multiPart, MediaType.MULTIPART_FORM_DATA_TYPE), FileStringResponse.class);
  }

  /**
   * Validate properties based on FileStatInfo
   */
  private void validateFileProperties(FileStatInfo fileStatInfo, String fileName, String filePerms)
  {
    System.out.printf("Validating file properties for File: %s against perms: %s%n", fileName, filePerms);
    System.out.printf("Got fileStatInfo:%n%s", fileStatInfo.toString());
    Assert.assertEquals(fileStatInfo.getAbsolutePath(), String.format("%s/%s", ROOT_DIR, fileName));
    Assert.assertEquals(fileStatInfo.getSize(), TEST_FILE_SIZE);
    Assert.assertEquals(fileStatInfo.getUid(), 1000);
    Assert.assertEquals(fileStatInfo.getGid(), 1000);
    Assert.assertEquals(fileStatInfo.getPerms(), filePerms);
    Assert.assertFalse(fileStatInfo.isDir());
    Assert.assertFalse(fileStatInfo.isLink());
    Assert.assertNotNull(fileStatInfo.getAccessTime());
    Assert.assertNotNull(fileStatInfo.getModifyTime());
  }
}
