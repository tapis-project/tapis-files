package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.files.api.BaseResourceConfig;
import edu.utexas.tacc.tapis.files.api.models.MkdirRequest;
import edu.utexas.tacc.tapis.files.api.models.NativeLinuxFaclRequest;
import edu.utexas.tacc.tapis.files.api.models.NativeLinuxOpRequest;
import edu.utexas.tacc.tapis.files.api.providers.FilePermissionsAuthz;
import edu.utexas.tacc.tapis.files.integration.transfers.IntegrationTestUtils;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCacheNoAuth;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.models.AclEntry;
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
import edu.utexas.tacc.tapis.shared.ssh.SshSessionPool;
import edu.utexas.tacc.tapis.sharedapi.jaxrs.filters.JWTValidateRequestFilter;
import edu.utexas.tacc.tapis.sharedapi.responses.TapisResponse;
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
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;

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
  private static class GetFaclResponse extends TapisResponse<List<AclEntry>> { }
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
  private static final String FACL_ROUTE = UTILS_ROUTE + "/facl";

  // mocking out the services
  private ServiceClients serviceClients;
  private SKClient skClient;
  private FilePermsService permsService;
  private SystemsCacheNoAuth systemsCacheNoAuth;
  private SystemsCache systemsCache;

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
    permsService = Mockito.mock(FilePermsService.class);
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
            .register(new AbstractBinder()
            {
              @Override
              protected void configure()
              {
                bind(serviceClients).to(ServiceClients.class);
                bind(tenantManager).to(TenantManager.class);
                bind(permsService).to(FilePermsService.class);
                bind(systemsCache).to(SystemsCache.class);
                bind(systemsCacheNoAuth).to(SystemsCacheNoAuth.class);
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
    IntegrationTestUtils.clearSshSessionPoolInstance();
    SshSessionPool.init();
    Mockito.reset(skClient);
    Mockito.reset(serviceClients);
    Mockito.reset(systemsCache, systemsCacheNoAuth);
    when(serviceClients.getClient(any(String.class), any(String.class), eq(SKClient.class))).thenReturn(skClient);
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
    when(systemsCacheNoAuth.getSystem(any(), any(), any())).thenReturn(testSystem);
    when(systemsCache.getSystem(any(), any(), any())).thenReturn(testSystem);
    when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);

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
    when(systemsCacheNoAuth.getSystem(any(), any(), any())).thenReturn(testSystem);
    when(systemsCache.getSystem(any(), any(), any())).thenReturn(testSystem);
    when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);
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
    when(systemsCacheNoAuth.getSystem(any(), any(), any())).thenReturn(testSystem);
    when(systemsCache.getSystem(any(), any(), any())).thenReturn(testSystem);
    when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);
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
    when(systemsCacheNoAuth.getSystem(any(), any(), any())).thenReturn(testSystem);
    when(systemsCache.getSystem(any(), any(), any())).thenReturn(testSystem);
    when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);
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

  @Test(dataProvider = "testSystemsProvider")
  public void testFileAclAddAndDelete(TapisSystem testSystem) throws Exception {
    when(systemsCacheNoAuth.getSystem(any(), any(), any())).thenReturn(testSystem);
    when(systemsCache.getSystem(any(), any(), any())).thenReturn(testSystem);
    when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);

    // list acls to add for test
    List<AclEntry> addAcls = Arrays.asList(
            new AclEntry(false, "user", "facluser1", "rwx"),
            new AclEntry(false, "user", "facluser2", "r-x"),
            new AclEntry(false, "user", "facluser3", "r-x"),
            new AclEntry(false, "group", "faclgrp1", "--x"),
            new AclEntry(false, "group", "faclgrp2", "r--"),
            new AclEntry(false, "group", "faclgrp3", "-w-"));

    List<AclEntry> removeAcls = Arrays.asList(
            new AclEntry(false, "user", "facluser2", null),
            new AclEntry(false, "user", "facluser3", null),
            new AclEntry(false, "group", "faclgrp2", null),
            new AclEntry(false, "group", "faclgrp3", null));

    List<AclEntry> afterRemoveAcls = Arrays.asList(
            new AclEntry(false, "user", "facluser1", "rwx"),
            new AclEntry(false, "group", "faclgrp1", "--x"));

    // create a couple of test files
    addTestFilesToSystem(testSystem, TEST_FILE1, 1);

    // add acls and check result
    setfaclAndCheck(testSystem, TEST_FILE1, FileUtilsService.NativeLinuxFaclOperation.ADD,
            FileUtilsService.NativeLinuxFaclRecursion.NONE, addAcls, addAcls);

    // remove acls and check result
    setfaclAndCheck(testSystem, TEST_FILE1, FileUtilsService.NativeLinuxFaclOperation.REMOVE,
            FileUtilsService.NativeLinuxFaclRecursion.NONE, removeAcls, afterRemoveAcls);

  }

  @Test(dataProvider = "testSystemsProvider")
  public void testFileAclAddAndDeleteWeirdFiles(TapisSystem testSystem) throws Exception {
    when(systemsCacheNoAuth.getSystem(any(), any(), any())).thenReturn(testSystem);
    when(systemsCache.getSystem(any(), any(), any())).thenReturn(testSystem);
    when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);

    // list acls to add for test
    List<AclEntry> addAcls = Arrays.asList(
            new AclEntry(false, "user", "facluser1", "rwx"),
            new AclEntry(false, "user", "facluser2", "r-x"),
            new AclEntry(false, "user", "facluser3", "r-x"),
            new AclEntry(false, "group", "faclgrp1", "--x"),
            new AclEntry(false, "group", "faclgrp2", "r--"),
            new AclEntry(false, "group", "faclgrp3", "-w-"));

    List<AclEntry> removeAcls = Arrays.asList(
            new AclEntry(false, "user", "facluser2", null),
            new AclEntry(false, "user", "facluser3", null),
            new AclEntry(false, "group", "faclgrp2", null),
            new AclEntry(false, "group", "faclgrp3", null));

    List<AclEntry> afterRemoveAcls = Arrays.asList(
            new AclEntry(false, "user", "facluser1", "rwx"),
            new AclEntry(false, "group", "faclgrp1", "--x"));

    // create a couple of test files
    String fileWithSingleQuote = "test'file";
    String fileWithSemiColon = "test;file";
    addTestFilesToSystem(testSystem, fileWithSingleQuote, 1);
    addTestFilesToSystem(testSystem, fileWithSemiColon, 1);

    // add acls and check result
    setfaclAndCheck(testSystem, fileWithSingleQuote, FileUtilsService.NativeLinuxFaclOperation.ADD,
            FileUtilsService.NativeLinuxFaclRecursion.NONE, addAcls, addAcls);
    setfaclAndCheck(testSystem, fileWithSemiColon, FileUtilsService.NativeLinuxFaclOperation.ADD,
            FileUtilsService.NativeLinuxFaclRecursion.NONE, addAcls, addAcls);

    // remove acls and check result
    setfaclAndCheck(testSystem, fileWithSingleQuote, FileUtilsService.NativeLinuxFaclOperation.REMOVE,
            FileUtilsService.NativeLinuxFaclRecursion.NONE, removeAcls, afterRemoveAcls);
    setfaclAndCheck(testSystem, fileWithSemiColon, FileUtilsService.NativeLinuxFaclOperation.REMOVE,
            FileUtilsService.NativeLinuxFaclRecursion.NONE, removeAcls, afterRemoveAcls);

  }

  @Test(dataProvider = "testSystemsProvider")
  public void testDirAclAddAndDelete(TapisSystem testSystem) throws Exception {
    when(systemsCacheNoAuth.getSystem(any(), any(), any())).thenReturn(testSystem);
    when(systemsCache.getSystem(any(), any(), any())).thenReturn(testSystem);
    when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);

    // list acls to add for test
    List<AclEntry> addAcls = Arrays.asList(
            new AclEntry(true, "user", "facluser1", "r-x"),
            new AclEntry(true, "user", "facluser2", "rwx"),
            new AclEntry(true, "user", "facluser3", "rw-"),
            new AclEntry(false, "user", "facluser1", "rwx"),
            new AclEntry(false, "user", "facluser2", "r-x"),
            new AclEntry(false, "user", "facluser3", "r-x"),
            new AclEntry(false, "group", "faclgrp1", "--x"),
            new AclEntry(false, "group", "faclgrp2", "r--"),
            new AclEntry(false, "group", "faclgrp3", "-w-"),
            new AclEntry(true, "group", "faclgrp1", "--x"),
            new AclEntry(true, "group", "faclgrp2", "r--"),
            new AclEntry(true, "group", "faclgrp3", "-w-"));

    List<AclEntry> removeAcls = Arrays.asList(
            new AclEntry(false, "user", "facluser2", null),
            new AclEntry(false, "user", "facluser3", null),
            new AclEntry(true, "user", "facluser2", null),
            new AclEntry(true, "user", "facluser3", null),
            new AclEntry(false, "group", "faclgrp2", null),
            new AclEntry(false, "group", "faclgrp3", null),
            new AclEntry(true, "group", "faclgrp2", null),
            new AclEntry(true, "group", "faclgrp3", null));

    List<AclEntry> afterRemoveAcls = Arrays.asList(
            new AclEntry(false, "user", "facluser1", "rwx"),
            new AclEntry(false, "group", "faclgrp1", "--x"),
            new AclEntry(true, "user", "facluser1", "r-x"),
            new AclEntry(true, "group", "faclgrp1", "--x"));

    // create a test directory and a couple of test files
    String testDirectory = "testFaclDir";
    String testFile1 = testDirectory + "/" + TEST_FILE1;
    String testFile2 = testDirectory + "/" + TEST_FILE2;
    makeDirectoryOnSystem(testSystem, testDirectory);
    addTestFilesToSystem(testSystem, testFile1, 1);
    addTestFilesToSystem(testSystem, testFile2, 2);

    setfaclAndCheck(testSystem, testDirectory, FileUtilsService.NativeLinuxFaclOperation.ADD,
            FileUtilsService.NativeLinuxFaclRecursion.NONE, addAcls, addAcls);
    // make sure we didn't inadvertently supply the recursive flag
    checkFacl(testSystem, testFile1, Collections.emptyList());
    checkFacl(testSystem, testFile2, Collections.emptyList());

    setfaclAndCheck(testSystem, testDirectory, FileUtilsService.NativeLinuxFaclOperation.REMOVE,
            FileUtilsService.NativeLinuxFaclRecursion.NONE, removeAcls, afterRemoveAcls);
    // make sure we didn't inadvertently supply the recursive flag
    checkFacl(testSystem, testFile1, Collections.emptyList());
    checkFacl(testSystem, testFile2, Collections.emptyList());
  }

  @Test(dataProvider = "testSystemsProvider")
  public void testFileAclDeleteAll(TapisSystem testSystem) throws Exception {
    when(systemsCacheNoAuth.getSystem(any(), any(), any())).thenReturn(testSystem);
    when(systemsCache.getSystem(any(), any(), any())).thenReturn(testSystem);
    when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);

    // list acls to add for test
    List<AclEntry> addAcls = Arrays.asList(
            new AclEntry(false, "user", "facluser1", "rwx"),
            new AclEntry(false, "user", "facluser2", "r-x"),
            new AclEntry(false, "user", "facluser3", "r-x"),
            new AclEntry(false, "group", "faclgrp1", "--x"),
            new AclEntry(false, "group", "faclgrp2", "r--"),
            new AclEntry(false, "group", "faclgrp3", "-w-"));

    // create a couple of test files
    addTestFilesToSystem(testSystem, TEST_FILE1, 1);

    // add acls and check result
    setfaclAndCheck(testSystem, TEST_FILE1, FileUtilsService.NativeLinuxFaclOperation.ADD,
            FileUtilsService.NativeLinuxFaclRecursion.NONE, addAcls, addAcls);

    // remove acls and check result
    setfaclAndCheck(testSystem, TEST_FILE1, FileUtilsService.NativeLinuxFaclOperation.REMOVE_ALL,
            FileUtilsService.NativeLinuxFaclRecursion.NONE, null, Collections.emptyList());

  }

  @Test(dataProvider = "testSystemsProvider")
  public void testDirAclDeleteAll(TapisSystem testSystem) throws Exception {
    when(systemsCacheNoAuth.getSystem(any(), any(), any())).thenReturn(testSystem);
    when(systemsCache.getSystem(any(), any(), any())).thenReturn(testSystem);
    when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);

    // list acls to add for test
    List<AclEntry> addAcls = Arrays.asList(
            new AclEntry(true, "user", "facluser1", "r-x"),
            new AclEntry(true, "user", "facluser2", "rwx"),
            new AclEntry(true, "user", "facluser3", "rw-"),
            new AclEntry(false, "user", "facluser1", "rwx"),
            new AclEntry(false, "user", "facluser2", "r-x"),
            new AclEntry(false, "user", "facluser3", "r-x"),
            new AclEntry(false, "group", "faclgrp1", "--x"),
            new AclEntry(false, "group", "faclgrp2", "r--"),
            new AclEntry(false, "group", "faclgrp3", "-w-"),
            new AclEntry(true, "group", "faclgrp1", "--x"),
            new AclEntry(true, "group", "faclgrp2", "r--"),
            new AclEntry(true, "group", "faclgrp3", "-w-"));

    // create a couple of test files
    String testDirectory = "testFaclDir";
    makeDirectoryOnSystem(testSystem, testDirectory);

    setfaclAndCheck(testSystem, testDirectory, FileUtilsService.NativeLinuxFaclOperation.ADD,
            FileUtilsService.NativeLinuxFaclRecursion.NONE, addAcls, addAcls);

    setfaclAndCheck(testSystem, testDirectory, FileUtilsService.NativeLinuxFaclOperation.REMOVE_ALL,
            FileUtilsService.NativeLinuxFaclRecursion.NONE, null, Collections.emptyList());
  }

  @Test(dataProvider = "testSystemsProvider")
  public void testDirAclDeleteDefault(TapisSystem testSystem) throws Exception {
    when(systemsCacheNoAuth.getSystem(any(), any(), any())).thenReturn(testSystem);
    when(systemsCache.getSystem(any(), any(), any())).thenReturn(testSystem);
    when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);

    // list acls to add for test
    List<AclEntry> addAcls = Arrays.asList(
            new AclEntry(true, "user", "facluser1", "r-x"),
            new AclEntry(true, "user", "facluser2", "rwx"),
            new AclEntry(true, "user", "facluser3", "rw-"),
            new AclEntry(false, "user", "facluser1", "rwx"),
            new AclEntry(false, "user", "facluser2", "r-x"),
            new AclEntry(false, "user", "facluser3", "r-x"),
            new AclEntry(false, "group", "faclgrp1", "--x"),
            new AclEntry(false, "group", "faclgrp2", "r--"),
            new AclEntry(false, "group", "faclgrp3", "-w-"),
            new AclEntry(true, "group", "faclgrp1", "--x"),
            new AclEntry(true, "group", "faclgrp2", "r--"),
            new AclEntry(true, "group", "faclgrp3", "-w-"));

    List<AclEntry> afterRemoveAcls = Arrays.asList(
            new AclEntry(false, "user", "facluser1", "rwx"),
            new AclEntry(false, "user", "facluser2", "r-x"),
            new AclEntry(false, "user", "facluser3", "r-x"),
            new AclEntry(false, "group", "faclgrp1", "--x"),
            new AclEntry(false, "group", "faclgrp2", "r--"),
            new AclEntry(false, "group", "faclgrp3", "-w-"));

    // create a couple of test files
    String testDirectory = "testFaclDir";
    makeDirectoryOnSystem(testSystem, testDirectory);

    setfaclAndCheck(testSystem, testDirectory, FileUtilsService.NativeLinuxFaclOperation.ADD,
            FileUtilsService.NativeLinuxFaclRecursion.NONE, addAcls, addAcls);

    setfaclAndCheck(testSystem, testDirectory, FileUtilsService.NativeLinuxFaclOperation.REMOVE_DEFAULT,
            FileUtilsService.NativeLinuxFaclRecursion.NONE, null, afterRemoveAcls);
  }

  @Test(dataProvider = "testSystemsProvider")
  public void testDirAclAddAndDeleteRecursive(TapisSystem testSystem) throws Exception {
    when(systemsCacheNoAuth.getSystem(any(), any(), any())).thenReturn(testSystem);
    when(systemsCache.getSystem(any(), any(), any())).thenReturn(testSystem);
    when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);

    // list acls to add for test
    List<AclEntry> addAcls = Arrays.asList(
            new AclEntry(true, "user", "facluser1", "r-x"),
            new AclEntry(true, "user", "facluser2", "rwx"),
            new AclEntry(true, "user", "facluser3", "rw-"),
            new AclEntry(false, "user", "facluser1", "rwx"),
            new AclEntry(false, "user", "facluser2", "r-x"),
            new AclEntry(false, "user", "facluser3", "r-x"),
            new AclEntry(false, "group", "faclgrp1", "--x"),
            new AclEntry(false, "group", "faclgrp2", "r--"),
            new AclEntry(false, "group", "faclgrp3", "-w-"),
            new AclEntry(true, "group", "faclgrp1", "--x"),
            new AclEntry(true, "group", "faclgrp2", "r--"),
            new AclEntry(true, "group", "faclgrp3", "-w-"));

    // Files will not have "default:" acls
    List<AclEntry> afterAddFileAcls = Arrays.asList(
            new AclEntry(false, "user", "facluser1", "rwx"),
            new AclEntry(false, "user", "facluser2", "r-x"),
            new AclEntry(false, "user", "facluser3", "r-x"),
            new AclEntry(false, "group", "faclgrp1", "--x"),
            new AclEntry(false, "group", "faclgrp2", "r--"),
            new AclEntry(false, "group", "faclgrp3", "-w-"));


    List<AclEntry> removeAcls = Arrays.asList(
            new AclEntry(true, "user", "facluser2", null),
            new AclEntry(true, "user", "facluser3", null),
            new AclEntry(false, "user", "facluser2", null),
            new AclEntry(false, "user", "facluser3", null),
            new AclEntry(false, "group", "faclgrp2", null),
            new AclEntry(false, "group", "faclgrp3", null),
            new AclEntry(true, "group", "faclgrp2", null),
            new AclEntry(true, "group", "faclgrp3", null));

    List<AclEntry> afterRemoveDirAcls = Arrays.asList(
            new AclEntry(true, "user", "facluser1", "r-x"),
            new AclEntry(false, "user", "facluser1", "rwx"),
            new AclEntry(false, "group", "faclgrp1", "--x"),
            new AclEntry(true, "group", "faclgrp1", "--x"));

    List<AclEntry> afterRemoveFileAcls = Arrays.asList(
            new AclEntry(false, "user", "facluser1", "rwx"),
            new AclEntry(false, "group", "faclgrp1", "--x"));

    // create a couple of test files and directories
    // testFaclDir/
    //   testfile1.txt
    //   testfile2.txt
    //   testFaclDir3/
    //     testfile3.txt
    //     testfile4.txt
    String testDirectory1 = "testFaclDir1";
    String testDirectory2 = testDirectory1 + "/testFaclDir2";
    String testFile1 = testDirectory1 + "/" + TEST_FILE1;
    String testFile2 = testDirectory1 + "/" + TEST_FILE2;
    String testFile3 = testDirectory2 + "/" + TEST_FILE3;
    String testFile4 = testDirectory2 + "/" + TEST_FILE4;

    makeDirectoryOnSystem(testSystem, testDirectory1);
    makeDirectoryOnSystem(testSystem, testDirectory2);
    addTestFilesToSystem(testSystem, testFile1, 1);
    addTestFilesToSystem(testSystem, testFile2, 1);
    addTestFilesToSystem(testSystem, testFile3, 1);
    addTestFilesToSystem(testSystem, testFile4, 1);

    setfaclAndCheck(testSystem, testDirectory1, FileUtilsService.NativeLinuxFaclOperation.ADD,
            FileUtilsService.NativeLinuxFaclRecursion.PHYSICAL, addAcls, addAcls);
    // setfaclAndCheck only checks the target path - now check perms on files and directories
    // in the directory path
    checkFacl(testSystem, testDirectory2, addAcls);
    checkFacl(testSystem, testFile1, afterAddFileAcls);
    checkFacl(testSystem, testFile2, afterAddFileAcls);
    checkFacl(testSystem, testFile3, afterAddFileAcls);
    checkFacl(testSystem, testFile4, afterAddFileAcls);

    setfaclAndCheck(testSystem, testDirectory1, FileUtilsService.NativeLinuxFaclOperation.REMOVE,
            FileUtilsService.NativeLinuxFaclRecursion.LOGICAL, removeAcls, afterRemoveDirAcls);
    // setfaclAndCheck only checks the target path - now check perms on files and directories
    // in the directory path
    checkFacl(testSystem, testDirectory2, afterRemoveDirAcls);
    checkFacl(testSystem, testFile1, afterRemoveFileAcls);
    checkFacl(testSystem, testFile2, afterRemoveFileAcls);
    checkFacl(testSystem, testFile3, afterRemoveFileAcls);
    checkFacl(testSystem, testFile4, afterRemoveFileAcls);
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
    FileStringResponse response = target(String.format("%s/%s/%s", OPS_ROUTE, urlEncode(system.getId()), urlEncode(fileName)))
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
            .post(Entity.entity(multiPart, MediaType.MULTIPART_FORM_DATA_TYPE), FileStringResponse.class);
  }

  /*
   * Make a directory on a system.
   */
  private void makeDirectoryOnSystem(TapisSystem testSystem, String path) throws Exception {
    MkdirRequest request = new MkdirRequest();
    request.setPath(path);

    target(String.format("%s/%s", OPS_ROUTE, testSystem.getId()))
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
            .post(Entity.json(request), TapisResponse.class);
  }

  private void setfaclAndCheck(TapisSystem testSystem, String path,
                               FileUtilsService.NativeLinuxFaclOperation operation,
                               FileUtilsService.NativeLinuxFaclRecursion recursion,
                               List<AclEntry> acls, List<AclEntry> expectedResult) throws Exception {
    // get the file acls for the path before anything is set
    GetFaclResponse getCmdResponse = getFacl(testSystem, path);
    Assert.assertNotNull(getCmdResponse);
    List<AclEntry> getCmdResult = getCmdResponse.getResult();
    Assert.assertNotNull(getCmdResult);

    // call setfacl to remove all default acls
    String aclString = null;
    if((acls != null) && (acls.size() > 0)){
      aclString = acls.stream().map(aclEntry -> {
        StringBuilder sb = new StringBuilder();
        if(aclEntry.isDefaultAcl()) {
          sb.append("default:");
        }
        String type = aclEntry.getType();
        if(type != null) {
          sb.append(type);
        }
        sb.append(":");
        String principal = aclEntry.getPrincipal();
        if(principal != null) {
          sb.append(aclEntry.getPrincipal());
        }
        sb.append(":");
        String permission = aclEntry.getPermissions();
        if(permission != null) {
          sb.append(permission);
        }
        return sb.toString();
      }).collect(Collectors.joining(","));
    }

    NativeLinuxOpResultResponse setCmdResponse = setFacl(testSystem, path,
            recursion,
            operation, aclString);
    Assert.assertNotNull(setCmdResponse);
    NativeLinuxOpResult setCmdResult = setCmdResponse.getResult();
    Assert.assertNotNull(setCmdResult);
    Assert.assertEquals(setCmdResult.getExitCode(), 0);

    checkFacl(testSystem, path, expectedResult);
  }

  private NativeLinuxOpResultResponse setFacl(TapisSystem testSystem, String path,
                                              FileUtilsService.NativeLinuxFaclRecursion recursion,
                                              FileUtilsService.NativeLinuxFaclOperation operation,
                                              String aclString) {
    NativeLinuxFaclRequest request = new NativeLinuxFaclRequest();
    request.setRecursionMethod(recursion);
    request.setOperation(operation);
    request.setAclString(aclString);
    NativeLinuxOpResultResponse result = target(String.format("%s/%s/%s", FACL_ROUTE, urlEncode(testSystem.getId()),
            urlEncode(path)))
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
            .post(Entity.json(request), NativeLinuxOpResultResponse.class);
    return result;
  }

  private GetFaclResponse getFacl(TapisSystem testSystem, String path) {
    GetFaclResponse result = target(String.format("%s/%s/%s", FACL_ROUTE,
            urlEncode(testSystem.getId()), urlEncode(path)))
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
            .get(GetFaclResponse.class);
    return result;
  }

  private void checkFacl(TapisSystem testSystem, String path, List<AclEntry> expectedAcls) throws Exception {
    // acls that get auto added after first add
    List<AclEntry> defaultAndAutoAddedAcls = Arrays.asList(
            new AclEntry(false, "user", null, null),
            new AclEntry(false, "group", null, null),
            new AclEntry(false, "other", null, null),
            new AclEntry(false, "mask", null, null),
            new AclEntry(true, "user", null, null),
            new AclEntry(true, "group", null, null),
            new AclEntry(true, "mask", null, null),
            new AclEntry(true, "other", null, null));

    TapisResponse<List<AclEntry>> response = getFacl(testSystem, path);
    Assert.assertNotNull(response);
    List<AclEntry> result = response.getResult();
    Assert.assertNotNull(result);

    for(AclEntry expectedEntry : expectedAcls) {
      Assert.assertTrue(result.remove(expectedEntry), "Missing ACL: " + expectedEntry);
    }

    // ignore auto-added acls if they were not specified in the expect list
    result = result.stream().filter( aclEntry -> {
      for(AclEntry ignoredAcl : defaultAndAutoAddedAcls) {
        if((aclEntry.isDefaultAcl() == ignoredAcl.isDefaultAcl()) &&
                (Objects.equals(aclEntry.getType(), ignoredAcl.getType())) &&
                (Objects.equals(aclEntry.getPrincipal(), ignoredAcl.getPrincipal()))) {
          return false;
        }
      }
      return true;
    }).collect(Collectors.toList());

    Assert.assertEquals(result.size(), 0, "Found unexpected ACLs: " + result);
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

  private String urlEncode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }
}
