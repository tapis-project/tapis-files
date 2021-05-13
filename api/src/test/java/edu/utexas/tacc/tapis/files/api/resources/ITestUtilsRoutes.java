package edu.utexas.tacc.tapis.files.api.resources;


import edu.utexas.tacc.tapis.files.api.BaseResourceConfig;
import edu.utexas.tacc.tapis.files.api.models.MkdirRequest;
import edu.utexas.tacc.tapis.files.api.models.MoveCopyOperation;
import edu.utexas.tacc.tapis.files.api.models.MoveCopyRequest;
import edu.utexas.tacc.tapis.files.api.providers.FilePermissionsAuthz;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.FileStatInfo;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService;
import edu.utexas.tacc.tapis.files.lib.services.FilePermsService;
import edu.utexas.tacc.tapis.files.lib.services.FileUtilsService;
import edu.utexas.tacc.tapis.files.lib.services.IFileOpsService;
import edu.utexas.tacc.tapis.files.lib.services.IFileUtilsService;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.shared.security.ServiceJWT;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.shared.ssh.SSHConnectionCache;
import edu.utexas.tacc.tapis.sharedapi.jaxrs.filters.JWTValidateRequestFilter;
import edu.utexas.tacc.tapis.sharedapi.responses.TapisResponse;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
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

/*
 * Tests for getStatInfo and runLinuxNativeOp (chmod, chown, chgrp)
 */
@Test(groups = {"integration"})
public class ITestUtilsRoutes extends BaseDatabaseIntegrationTest
{
  private Logger log = LoggerFactory.getLogger(ITestUtilsRoutes.class);

  private static class FileStatInfoResponse extends TapisResponse<FileStatInfo> { }
  private static class FileListResponse extends TapisResponse<List<FileInfo>> { }
  private static class FileStringResponse extends TapisResponse<String> { }

  private TapisSystem testSystemSSH;
  private List<TapisSystem> testSystems = new ArrayList<>();

  // mocking out the services
  private ServiceClients serviceClients;
  private SystemsClient systemsClient;
  private SKClient skClient;
  private ServiceJWT serviceJWT;
  private final FilePermsService permsService = Mockito.mock(FilePermsService.class);

  /*
   * Private constructor to create and setup systems used in tests
   */
  private ITestUtilsRoutes() throws Exception
  {
    //SSH system with username/password
    Credential sshCreds = new Credential();
    sshCreds.setAccessKey("testuser");
    sshCreds.setPassword("password");
    testSystemSSH = new TapisSystem();
    testSystemSSH.setSystemType(SystemTypeEnum.LINUX);
    testSystemSSH.setAuthnCredential(sshCreds);
    testSystemSSH.setHost("localhost");
    testSystemSSH.setPort(2222);
    testSystemSSH.setRootDir("/data/home/testuser/");
    testSystemSSH.setId("testSystem");
    testSystemSSH.setEffectiveUserId("testuser");

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
    //JWT validation
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
                bindAsContract(SystemsCache.class).in(Singleton.class);
                bind(FileOpsService.class).to(IFileOpsService.class).in(Singleton.class);
                bind(FileUtilsService.class).to(IFileUtilsService.class).in(Singleton.class);
                bindAsContract(RemoteDataClientFactory.class);
                bind(serviceContext).to(ServiceContext.class);
                bind(new SSHConnectionCache(1, TimeUnit.MINUTES)).to(SSHConnectionCache.class);
              }
            });

    app.register(OperationsApiResource.class);
    app.register(UtilsLinuxApiResource.class);
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
  public void cleanup() throws Exception
  {
    testSystems.forEach((sys) -> {
      try
      {
        when(skClient.isPermitted(any(), any(), any())).thenReturn(true);
        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(sys);
        target("/v3/files/ops/testSystem/")
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
                .delete(FileStringResponse.class);
      } catch (Exception ex)
      {
        log.error(ex.getMessage(), ex);
      }
    });
  }

  /**
   * Test listing of files (duplicated from ITestOpsRoutes. TODO/TBD: remove)
   */
  @Test(dataProvider = "testSystemsProvider")
  public void testGetList(TapisSystem testSystem) throws Exception
  {
    when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
    addTestFilesToSystem(testSystem, "testfile1.txt", 10 * 1024);
    FileListResponse response = target("/v3/files/ops/testSystem/")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .get(FileListResponse.class);
    FileInfo file = response.getResult().get(0);
    Assert.assertEquals(file.getName(), "testfile1.txt");
    Assert.assertEquals(file.getSize(), 10 * 1024);
    Assert.assertNotNull(file.getUri());
  }

  /*
   * Test getStatInfo
   */
  @Test(dataProvider = "testSystemsProvider")
  public void testGetStatInfo(TapisSystem testSystem) throws Exception
  {
    when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
    addTestFilesToSystem(testSystem, "testfile1.txt", 10 * 1024);
    FileStatInfoResponse response = target("/v3/files/utils/linux/testSystem/testfile1.txt")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getServiceJwt())
            .header("x-tapis-user", "testuser1")
            .header("x-tapis-tenant", "dev")
            .get(FileStatInfoResponse.class);
    FileStatInfo fileStatInfo = response.getResult();
    Assert.assertEquals(fileStatInfo.getAbsolutePath(), "/data/home/testuser/testfile1.txt");
    Assert.assertEquals(fileStatInfo.getSize(), 10 * 1024);
  }

//    @Test(dataProvider = "testSystemsProvider")
//    public void testGetListWithLimitAndOffset(TapisSystem testSystem) throws Exception {
//        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
//
//        addTestFilesToSystem(testSystem, "testfile1.txt", 10 * 1024);
//        addTestFilesToSystem(testSystem, "testfile2.txt", 10 * 1024);
//        addTestFilesToSystem(testSystem, "testfile3.txt", 10 * 1024);
//        addTestFilesToSystem(testSystem, "testfile4.txt", 10 * 1024);
//        addTestFilesToSystem(testSystem, "testfile5.txt", 10 * 1024);
//        addTestFilesToSystem(testSystem, "testfile6.txt", 10 * 1024);
//        addTestFilesToSystem(testSystem, "testfile7.txt", 10 * 1024);
//        addTestFilesToSystem(testSystem, "testfile8.txt", 10 * 1024);
//
//        FileListResponse response = target("/v3/files/ops/testSystem/")
//            .queryParam("limit", "2")
//            .queryParam("offset", "2")
//            .request()
//            .accept(MediaType.APPLICATION_JSON)
//            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
//            .get(FileListResponse.class);
//        List<FileInfo> listing = response.getResult();
//        Assert.assertEquals(listing.size(), 2);
//        Assert.assertEquals(listing.get(0).getName(), "testfile3.txt");
//    }
//
//
//    @Test(dataProvider = "testSystemsProvider")
//    public void testGetListNoAuthz(TapisSystem testSystem) throws Exception {
//        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
//        addTestFilesToSystem(testSystem, "testfile1.txt", 10 * 1024);
//        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(false);
//        Response response = target("/v3/files/ops/testSystem/testfile1.txt")
//            .request()
//            .accept(MediaType.APPLICATION_JSON)
//            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
//            .get();
//        Assert.assertEquals(response.getStatus(), 403);
//    }
//
//    @Test(dataProvider = "testSystemsProvider")
//    public void testDelete(TapisSystem testSystem) throws Exception {
//        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
//        addTestFilesToSystem(testSystem, "testfile1.txt", 10 * 1024);
//        addTestFilesToSystem(testSystem, "testfile2.txt", 10 * 1024);
//        addTestFilesToSystem(testSystem, "dir1/testfile3.txt", 10 * 1024);
//
//        FileStringResponse response = target("/v3/files/ops/testSystem/dir1/testfile3.txt")
//            .request()
//            .accept(MediaType.APPLICATION_JSON)
//            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
//            .delete(FileStringResponse.class);
//
//        assertThrowsNotFoundForTestUser1("dir1/testfile3.txt");
//    }
//
//    @Test(dataProvider = "testSystemsProvider")
//    public void testCopyFile(TapisSystem testSystem) throws Exception {
//        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
//        addTestFilesToSystem(testSystem, "sample1.txt", 10 * 1024);
//        MoveCopyRequest request = new MoveCopyRequest();
//        request.setOperation(MoveCopyOperation.COPY);
//        request.setNewPath("/filestest/sample1.txt");
//
//        FileStringResponse response = target("/v3/files/ops/testSystem/sample1.txt")
//            .request()
//            .accept(MediaType.APPLICATION_JSON)
//            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
//            .put(Entity.json(request), FileStringResponse.class);
//
//        List<FileInfo> listing = doListing(testSystem.getId(), "filestest/sample1.txt", getJwtForUser("dev", "testuser1"));
//        Assert.assertEquals(listing.size(), 1);
//        Assert.assertEquals(listing.get(0).getPath(), "filestest/sample1.txt");
//    }
//
//    @Test(dataProvider = "testSystemsProvider")
//    public void testCopyFileShould404(TapisSystem testSystem) throws Exception {
//        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
//        addTestFilesToSystem(testSystem, "sample1.txt", 10 * 1024);
//        MoveCopyRequest request = new MoveCopyRequest();
//        request.setOperation(MoveCopyOperation.COPY);
//        request.setNewPath("/filestest/sample1.txt");
//
//
//
//        Assert.assertThrows(NotFoundException.class, ()->target("/v3/files/ops/testSystem/NOT-THERE.txt")
//            .request()
//            .accept(MediaType.APPLICATION_JSON)
//            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
//            .put(Entity.json(request), FileStringResponse.class) );
//
//    }
//
//    @Test(dataProvider = "testSystemsProvider")
//    public void testRenameFile2(TapisSystem testSystem) throws Exception {
//        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
//        addTestFilesToSystem(testSystem, "testfile1.txt", 10 * 1024);
//
//        MoveCopyRequest request = new MoveCopyRequest();
//        request.setOperation(MoveCopyOperation.MOVE);
//        request.setNewPath("renamed");
//
//        FileStringResponse response = target("/v3/files/ops/testSystem/testfile1.txt")
//            .request()
//            .accept(MediaType.APPLICATION_JSON)
//            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
//            .put(Entity.json(request), FileStringResponse.class);
//
//        List<FileInfo> listing = doListing(testSystem.getId(), "renamed", getJwtForUser("dev", "testuser1"));
//        Assert.assertEquals(listing.size(), 1);
//        Assert.assertEquals(listing.get(0).getPath(), "renamed");
//    }
//
//    @Test(dataProvider = "testSystemsProvider")
//    public void testRenameManyObjects1(TapisSystem testSystem) throws Exception {
//        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
//        addTestFilesToSystem(testSystem, "test1.txt", 10 * 1024);
//        addTestFilesToSystem(testSystem, "test2.txt", 10 * 1024);
//        addTestFilesToSystem(testSystem, "dir1/1.txt", 10 * 1024);
//        addTestFilesToSystem(testSystem, "dir1/dir2/2.txt", 10 * 1024);
//        addTestFilesToSystem(testSystem, "dir1/dir2/dir3/3.txt", 10 * 1024);
//        addTestFilesToSystem(testSystem, "dir1/dir2/dir3/dir4.txt", 10 * 1024);
//
//        MoveCopyRequest request = new MoveCopyRequest();
//        request.setOperation(MoveCopyOperation.MOVE);
//        request.setNewPath("renamed");
//
//        FileStringResponse response = target("/v3/files/ops/testSystem/dir1/")
//            .request()
//            .accept(MediaType.APPLICATION_JSON)
//            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
//            .put(Entity.json(request), FileStringResponse.class);
//
//
//        assertThrowsNotFoundForTestUser1("/dir1/1.txt");
//        Assert.assertTrue(doListing(testSystem.getId(), "/renamed", getJwtForUser("dev", "testuser1")).size() > 0);
//    }
//
//
//    /**
//     * Tests of objects deeper in the tree are renamed properly.
//     *
//     * @throws Exception
//     */
//    @Test(dataProvider = "testSystemsProvider")
//    public void testRenameManyObjects2(TapisSystem testSystem) throws Exception {
//        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
//
//        addTestFilesToSystem(testSystem, "test1.txt", 10 * 1024);
//        addTestFilesToSystem(testSystem, "test2.txt", 10 * 1024);
//        addTestFilesToSystem(testSystem, "dir1/1.txt", 10 * 1024);
//        addTestFilesToSystem(testSystem, "dir1/dir2/2.txt", 10 * 1024);
//        addTestFilesToSystem(testSystem, "dir1/dir2/dir3/3.txt", 10 * 1024);
//        addTestFilesToSystem(testSystem, "dir1/dir2/dir3/dir4.txt", 10 * 1024);
//
//        MoveCopyRequest request = new MoveCopyRequest();
//        request.setOperation(MoveCopyOperation.MOVE);
//        request.setNewPath("renamed");
//
//        FileStringResponse response = target("/v3/files/ops/testSystem/dir1/dir2/")
//            .request()
//            .accept(MediaType.APPLICATION_JSON)
//            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
//            .put(Entity.json(request), FileStringResponse.class);
//
//        List<FileInfo> listing = doListing(testSystem.getId(), "dir1/1.txt", getJwtForUser("dev", "testuser1"));
//        Assert.assertEquals(listing.size(), 1);
//        listing = doListing(testSystem.getId(), "dir1/1.txt", getJwtForUser("dev", "testuser1"));
//        Assert.assertEquals(listing.size(), 1);
//        assertThrowsNotFoundForTestUser1("/dir1/dir2/");
//    }
//
//
//    @Test(dataProvider = "testSystemsProvider")
//    public void testDeleteManyObjects(TapisSystem testSystem) throws Exception {
//        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
//        addTestFilesToSystem(testSystem, "test1.txt", 10 * 1024);
//        addTestFilesToSystem(testSystem, "test2.txt", 10 * 1024);
//        addTestFilesToSystem(testSystem, "dir1/1.txt", 10 * 1024);
//        addTestFilesToSystem(testSystem, "dir1/dir2/2.txt", 10 * 1024);
//        addTestFilesToSystem(testSystem, "dir1/dir2/dir3/3.txt", 10 * 1024);
//        addTestFilesToSystem(testSystem, "dir1/dir2/dir3/dir4.txt", 10 * 1024);
//
//        FileStringResponse response = target("/v3/files/ops/testSystem/dir1/dir2/")
//            .request()
//            .accept(MediaType.APPLICATION_JSON)
//            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
//            .delete(FileStringResponse.class);
//
//        List<FileInfo> listing = doListing(testSystem.getId(), "dir1/1.txt", getJwtForUser("dev", "testuser1"));
//        Assert.assertEquals(listing.size(), 1);
//        listing = doListing(testSystem.getId(), "dir1", getJwtForUser("dev", "testuser1"));
//        Assert.assertEquals(listing.size(), 1);
//        assertThrowsNotFoundForTestUser1("/dir1/dir2/");
//
//    }
//
//    @Test(dataProvider = "testSystemsProvider")
//    public void testInsertFile(TapisSystem testSystem) throws Exception {
//        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
//        InputStream inputStream = makeFakeFile(10 * 1024);
//        File tempFile = File.createTempFile("tempfile", null);
//        tempFile.deleteOnExit();
//        FileOutputStream fos = new FileOutputStream(tempFile);
//        fos.write(inputStream.readAllBytes());
//        fos.close();
//        FileDataBodyPart filePart = new FileDataBodyPart("file", tempFile);
//        FormDataMultiPart form = new FormDataMultiPart();
//        FormDataMultiPart multiPart = (FormDataMultiPart) form.bodyPart(filePart);
//        FileStringResponse response = target("/v3/files/ops/testSystem/test-inserted.txt")
//            .request()
//            .accept(MediaType.APPLICATION_JSON)
//            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
//            .post(Entity.entity(multiPart, MediaType.MULTIPART_FORM_DATA_TYPE), FileStringResponse.class);
//
//        Assert.assertEquals(response.getStatus(), "success");
//
//        FileListResponse listing = target("/v3/files/ops/testSystem/test-inserted.txt")
//            .request()
//            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
//            .get(FileListResponse.class);
//        Assert.assertEquals(listing.getResult().size(), 1);
//        Assert.assertEquals(listing.getResult().get(0).getName(), "test-inserted.txt");
//    }
//
//    @Test(dataProvider = "testSystemsProvider")
//    public void testMkdir(TapisSystem testSystem) throws Exception {
//        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
//
//        MkdirRequest req = new MkdirRequest();
//        req.setPath("newDirectory");
//
//        FileStringResponse response = target("/v3/files/ops/testSystem/")
//            .request()
//            .accept(MediaType.APPLICATION_JSON)
//            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
//            .post(Entity.json(req), FileStringResponse.class);
//
//        FileListResponse listing = target("/v3/files/ops/testSystem/")
//            .request()
//            .accept(MediaType.APPLICATION_JSON)
//            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
//            .get(FileListResponse.class);
//
//        Assert.assertEquals(listing.getResult().size(), 1);
//        Assert.assertEquals(listing.getResult().get(0).getName(), "newDirectory");
//        Assert.assertEquals(listing.getResult().get(0).getType(), "dir");
//    }
//
//
//    @Test(dataProvider = "testSystemsProvider")
//    public void testMkdirNoSlash(TapisSystem testSystem) throws Exception {
//        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
//        MkdirRequest req = new MkdirRequest();
//        req.setPath("newDirectory");
//
//        FileStringResponse response = target("/v3/files/ops/testSystem/")
//            .request()
//            .accept(MediaType.APPLICATION_JSON)
//            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
//            .post(Entity.json(req), FileStringResponse.class);
//
//        FileListResponse listing = target("/v3/files/ops/testSystem/")
//            .request()
//            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
//            .get(FileListResponse.class);
//
//        Assert.assertEquals(listing.getResult().size(), 1);
//        Assert.assertEquals(listing.getResult().get(0).getName(), "newDirectory");
//        Assert.assertEquals(listing.getResult().get(0).getType(), "dir");
//    }
//
//
//    public String[] mkdirData() {
//        return new String[]{
//            "//newDirectory///test",
//            "newDirectory///test/",
//            "newDirectory/test"
//        };
//    }
//
//    @DataProvider(name="mkdirDataProvider")
//    public Object[] mkdirDataProvider() {
//        List<String> directories = Arrays.asList(mkdirData());
//        List<Pair<String, TapisSystem>> out = testSystems.stream().flatMap(sys -> directories.stream().map(j -> Pair.of(j, sys)))
//            .collect(Collectors.toList());
//        return out.toArray();
//
//    }
//
//
//    @Test(dataProvider = "mkdirDataProvider")
//    public void testMkdirWithSlashes(Pair<String, TapisSystem> inputs) throws Exception {
//        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(inputs.getRight());
//
//        MkdirRequest req = new MkdirRequest();
//        req.setPath(inputs.getLeft());
//
//        FileStringResponse response = target("/v3/files/ops/testSystem/")
//            .request()
//            .accept(MediaType.APPLICATION_JSON)
//            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
//            .post(Entity.json(req), FileStringResponse.class);
//
//        FileListResponse listing = target("/v3/files/ops/testSystem/newDirectory/")
//            .request()
//            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
//            .get(FileListResponse.class);
//        Assert.assertEquals(listing.getResult().size(), 1);
//    }
//
//    @DataProvider(name = "mkdirBadDataProvider")
//    public Object[] mkdirBadDataProvider() {
//        String[] mkdirData =  new String[]{
//            "/newDirectory//../test",
//            "newDirectory/../../test/",
//            "../newDirectory/test",
//            "newDirectory/..test"
//        };
//        List<String> directories = Arrays.asList(mkdirData);
//        List<Pair<String, TapisSystem>> out = testSystems.stream().flatMap(sys -> directories.stream().map(j -> Pair.of(j, sys)))
//            .collect(Collectors.toList());
//        return out.toArray();
//    }
//
//
//    /**
//     * All the funky paths should get cleaned up
//     * @param inputs
//     * @throws Exception
//     */
//    @Test(dataProvider = "mkdirBadDataProvider")
//    public void testMkdirWithBadData(Pair<String, TapisSystem> inputs) throws Exception {
//        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(inputs.getRight());
//        MkdirRequest req = new MkdirRequest();
//        req.setPath(inputs.getLeft());
//        Response response = target("/v3/files/ops/testSystem/")
//            .request()
//            .accept(MediaType.APPLICATION_JSON)
//            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
//            .post(Entity.json(req), Response.class);
//
//        Assert.assertEquals(response.getStatus(), 400);
//    }

  // =======================================
  // ====== Private methods ================
  // =======================================

  private List<FileInfo> doListing(String systemId, String path, String userJwt) {
    FileListResponse response = target("/v3/files/ops/" + systemId +"/" + path)
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", userJwt)
            .get(FileListResponse.class);
    return response.getResult();
  }

  private void assertThrowsNotFoundForTestUser1(String path)
  {
    Assert.assertThrows(NotFoundException.class, ()->{
      target("/v3/files/utils/testSystem/" + path)
              .request()
              .accept(MediaType.APPLICATION_JSON)
              .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
              .get(FileStringResponse.class);
    });
  }

  private InputStream makeFakeFile(int size) {
    byte[] b = new byte[size];
    new Random().nextBytes(b);
    InputStream is = new ByteArrayInputStream(b);
    return is;
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
    FileStringResponse response = target("/v3/files/ops/" + system.getId() + "/" + fileName)
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .post(Entity.entity(multiPart, MediaType.MULTIPART_FORM_DATA_TYPE), FileStringResponse.class);
  }
}
