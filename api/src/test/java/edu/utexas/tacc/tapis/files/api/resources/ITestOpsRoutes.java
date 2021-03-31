package edu.utexas.tacc.tapis.files.api.resources;


import edu.utexas.tacc.tapis.files.api.BaseResourceConfig;
import edu.utexas.tacc.tapis.files.api.models.MkdirRequest;
import edu.utexas.tacc.tapis.files.api.models.MoveCopyRenameOperation;
import edu.utexas.tacc.tapis.files.api.models.MoveCopyRenameRequest;
import edu.utexas.tacc.tapis.files.api.providers.FilePermissionsAuthz;
import edu.utexas.tacc.tapis.files.lib.caches.FilePermsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService;
import edu.utexas.tacc.tapis.files.lib.services.FilePermsService;
import edu.utexas.tacc.tapis.files.lib.services.IFileOpsService;
import edu.utexas.tacc.tapis.files.lib.utils.TenantCacheFactory;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.security.ServiceJWT;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.shared.ssh.SSHConnectionCache;
import edu.utexas.tacc.tapis.sharedapi.jaxrs.filters.JWTValidateRequestFilter;
import edu.utexas.tacc.tapis.sharedapi.responses.TapisResponse;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import edu.utexas.tacc.tapis.systems.client.gen.model.TransferMethodEnum;
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
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

@Test(groups = {"integration"})
public class ITestOpsRoutes extends BaseDatabaseIntegrationTest {

    private Logger log = LoggerFactory.getLogger(ITestOpsRoutes.class);
    private static class FileListResponse extends TapisResponse<List<FileInfo>> {
    }

    private static class FileStringResponse extends TapisResponse<String> {
    }

    private TSystem testSystemSSH;
    private TSystem testSystemS3;
    private List<TSystem> testSystems = new ArrayList<>();

    private Credential creds;

    // mocking out the services
    private SystemsClient systemsClient;
    private SKClient skClient;
    private ServiceJWT serviceJWT;



    private ITestOpsRoutes() throws Exception {
        //SSH system with username/password
        Credential sshCreds = new Credential();
        sshCreds.setAccessKey("testuser");
        sshCreds.setPassword("password");
        testSystemSSH = new TSystem();
        testSystemSSH.setAuthnCredential(sshCreds);
        testSystemSSH.setHost("localhost");
        testSystemSSH.setPort(2222);
        testSystemSSH.setRootDir("/data/home/testuser/");
        testSystemSSH.setId("testSystem");
        testSystemSSH.setEffectiveUserId("testuser");
        List<TransferMethodEnum> tMechs = new ArrayList<>();
        tMechs.add(TransferMethodEnum.SFTP);
        testSystemSSH.setTransferMethods(tMechs);

        creds = new Credential();
        creds.setAccessKey("user");
        creds.setAccessSecret("password");
        testSystemS3 = new TSystem();
        testSystemS3.setOwner("testuser1");
        testSystemS3.setHost("http://localhost");
        testSystemS3.setPort(9000);
        testSystemS3.setBucketName("test");
        testSystemS3.setId("testSystem");
        testSystemS3.setAuthnCredential(creds);
        testSystemS3.setRootDir("/");
        List<TransferMethodEnum> transferMechs = new ArrayList<>();
        transferMechs.add(TransferMethodEnum.S3);
        testSystemS3.setTransferMethods(transferMechs);

        testSystems.add(testSystemSSH);
        testSystems.add(testSystemS3);

    }

    @Override
    protected ResourceConfig configure() {
        enable(TestProperties.LOG_TRAFFIC);
        enable(TestProperties.DUMP_ENTITY);
        forceSet(TestProperties.CONTAINER_PORT, "0");
        skClient = Mockito.mock(SKClient.class);
        systemsClient = Mockito.mock(SystemsClient.class);
        serviceJWT = Mockito.mock(ServiceJWT.class);
        JWTValidateRequestFilter.setSiteId("tacc");
        JWTValidateRequestFilter.setService("files");
        //JWT validation
        ResourceConfig app = new BaseResourceConfig()
            .register(JWTValidateRequestFilter.class)
            .register(FilePermissionsAuthz.class)
            .register(new AbstractBinder() {
                @Override
                protected void configure() {
                    bind(systemsClient).to(SystemsClient.class);
                    bind(skClient).to(SKClient.class);
                    bindFactory(TenantCacheFactory.class).to(TenantManager.class).in(Singleton.class);
                    bind(serviceJWT).to(ServiceJWT.class);
                    bindAsContract(FilePermsService.class).in(Singleton.class);
                    bindAsContract(FilePermsCache.class).in(Singleton.class);
                    bindAsContract(SystemsCache.class).in(Singleton.class);
                    bind(FileOpsService.class).to(IFileOpsService.class).in(Singleton.class);
                    bindAsContract(RemoteDataClientFactory.class);
                    bind(new SSHConnectionCache(1, TimeUnit.MINUTES)).to(SSHConnectionCache.class);
                }
            });

        app.register(OperationsApiResource.class);
        return app;
    }

    @BeforeClass
    @Override
    public void setUp() throws Exception {
        super.setUp();
    }


    /**
     * This is silly, but TestNG requires this to be an Object[]
     * @return
     */
    @DataProvider(name="testSystemsProvider")
    public Object[] testSystemsProvider() {
        return testSystems.toArray();
    }


    private void assertThrowsNotFoundForTestUser1(String path) {
        Assert.assertThrows(NotFoundException.class, ()->{
            target("/v3/files/ops/testSystem/" + path)
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
                .get(FileStringResponse.class);
        });
    }

    private List<FileInfo> doListing(String systemId, String path, String userJwt) {
        FileListResponse response = target("/v3/files/ops/" + systemId +"/" + path)
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", userJwt)
            .get(FileListResponse.class);
        return response.getResult();
    }


    // Needed for the test client to be able to use Mutlipart/form posts;
    @Override
    protected void configureClient(ClientConfig config) {
        config.register(MultiPartFeature.class);
   }

    @BeforeMethod
    public void initMocks() throws Exception {
        when(skClient.isPermitted(any(), any(), any())).thenReturn(true);
    }




    @BeforeMethod
    @AfterMethod
    public void cleanup() throws Exception {
        testSystems.forEach( (sys)-> {
            try {
                when(skClient.isPermitted(any(), any(), any())).thenReturn(true);
                when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(sys);
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

    @Test(dataProvider = "testSystemsProvider")
    public void testGetList(TSystem testSystem) throws Exception {
        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
        ;
        addTestFilesToBucket(testSystem, "testfile1.txt", 10 * 1024);
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

    @Test(dataProvider = "testSystemsProvider")
    public void testGetListWithObo(TSystem testSystem) throws Exception {
        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
        addTestFilesToBucket(testSystem, "testfile1.txt", 10 * 1024);
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

    private InputStream makeFakeFile(int size) {
        byte[] b = new byte[size];
        new Random().nextBytes(b);
        InputStream is = new ByteArrayInputStream(b);
        return is;
    }

    private void addTestFilesToBucket(TSystem system, String fileName, int fileSize) throws Exception {
        InputStream inputStream = makeFakeFile(fileSize);
        File tempFile = File.createTempFile("tempfile", null);
        tempFile.deleteOnExit();
        FileOutputStream fos = new FileOutputStream(tempFile);
        fos.write(inputStream.readAllBytes());
        fos.close();
        FileDataBodyPart filePart = new FileDataBodyPart("file", tempFile);
        FormDataMultiPart form = new FormDataMultiPart();
        FormDataMultiPart multiPart = (FormDataMultiPart) form.bodyPart(filePart);
        FileStringResponse response = target("/v3/files/ops/" + system.getId() + "/" + fileName)
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .post(Entity.entity(multiPart, MediaType.MULTIPART_FORM_DATA_TYPE), FileStringResponse.class);
    }




    @Test(dataProvider = "testSystemsProvider")
    public void testGetListWithLimitAndOffset(TSystem testSystem) throws Exception {
        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);

        addTestFilesToBucket(testSystem, "testfile1.txt", 10 * 1024);
        addTestFilesToBucket(testSystem, "testfile2.txt", 10 * 1024);
        addTestFilesToBucket(testSystem, "testfile3.txt", 10 * 1024);
        addTestFilesToBucket(testSystem, "testfile4.txt", 10 * 1024);
        addTestFilesToBucket(testSystem, "testfile5.txt", 10 * 1024);
        addTestFilesToBucket(testSystem, "testfile6.txt", 10 * 1024);
        addTestFilesToBucket(testSystem, "testfile7.txt", 10 * 1024);
        addTestFilesToBucket(testSystem, "testfile8.txt", 10 * 1024);



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
    public void testGetListNoAuthz(TSystem testSystem) throws Exception {
        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
        addTestFilesToBucket(testSystem, "testfile1.txt", 10 * 1024);
        when(skClient.isPermitted(any(), any(String.class), any(String.class))).thenReturn(false);
        Response response = target("/v3/files/ops/testSystem/testfile1.txt")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .get();
        Assert.assertEquals(response.getStatus(), 403);
    }

    @Test(dataProvider = "testSystemsProvider")
    public void testDelete(TSystem testSystem) throws Exception {
        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
        addTestFilesToBucket(testSystem, "testfile1.txt", 10 * 1024);
        addTestFilesToBucket(testSystem, "testfile2.txt", 10 * 1024);
        addTestFilesToBucket(testSystem, "dir1/testfile3.txt", 10 * 1024);

        FileStringResponse response = target("/v3/files/ops/testSystem/dir1/testfile3.txt")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .delete(FileStringResponse.class);

        assertThrowsNotFoundForTestUser1("dir1/testfile3.txt");


    }

    @Test(dataProvider = "testSystemsProvider")
    public void testCopyFile(TSystem testSystem) throws Exception {
        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
        addTestFilesToBucket(testSystem, "sample1.txt", 10 * 1024);
        MoveCopyRenameRequest request = new MoveCopyRenameRequest();
        request.setOperation(MoveCopyRenameOperation.COPY);
        request.setNewPath("/filestest/sample1.txt");

        FileStringResponse response = target("/v3/files/ops/testSystem/sample1.txt")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .put(Entity.json(request), FileStringResponse.class);

        List<FileInfo> listing = doListing(testSystem.getId(), "filestest/sample1.txt", getJwtForUser("dev", "testuser1"));
        Assert.assertEquals(listing.size(), 1);
        Assert.assertEquals(listing.get(0).getPath(), "filestest/sample1.txt");
    }

    @Test(dataProvider = "testSystemsProvider")
    public void testCopyFileShould404(TSystem testSystem) throws Exception {
        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
        addTestFilesToBucket(testSystem, "sample1.txt", 10 * 1024);
        MoveCopyRenameRequest request = new MoveCopyRenameRequest();
        request.setOperation(MoveCopyRenameOperation.COPY);
        request.setNewPath("/filestest/sample1.txt");



        Assert.assertThrows(NotFoundException.class, ()->target("/v3/files/ops/testSystem/NOT-THERE.txt")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .put(Entity.json(request), FileStringResponse.class) );

    }

    @Test(dataProvider = "testSystemsProvider")
    public void testRenameFile(TSystem testSystem) throws Exception {
        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
        addTestFilesToBucket(testSystem, "testfile1.txt", 10 * 1024);
        addTestFilesToBucket(testSystem, "testfile2.txt", 10 * 1024);
        addTestFilesToBucket(testSystem, "dir1/testfile3.txt", 10 * 1024);

        MoveCopyRenameRequest request = new MoveCopyRenameRequest();
        request.setOperation(MoveCopyRenameOperation.RENAME);
        request.setNewPath("renamed");

        FileStringResponse response = target("/v3/files/ops/testSystem/dir1/testfile3.txt")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .put(Entity.json(request), FileStringResponse.class);

        List<FileInfo> listing = doListing(testSystem.getId(), "dir1/renamed", getJwtForUser("dev", "testuser1"));
        Assert.assertEquals(listing.size(), 1);
        Assert.assertEquals(listing.get(0).getPath(), "dir1/renamed");
    }

    @Test(dataProvider = "testSystemsProvider")
    public void testRenameManyObjects1(TSystem testSystem) throws Exception {
        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
        addTestFilesToBucket(testSystem, "test1.txt", 10 * 1024);
        addTestFilesToBucket(testSystem, "test2.txt", 10 * 1024);
        addTestFilesToBucket(testSystem, "dir1/1.txt", 10 * 1024);
        addTestFilesToBucket(testSystem, "dir1/dir2/2.txt", 10 * 1024);
        addTestFilesToBucket(testSystem, "dir1/dir2/dir3/3.txt", 10 * 1024);
        addTestFilesToBucket(testSystem, "dir1/dir2/dir3/dir4.txt", 10 * 1024);

        MoveCopyRenameRequest request = new MoveCopyRenameRequest();
        request.setOperation(MoveCopyRenameOperation.RENAME);
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
     * Tests of objects deeper in the tree are renamed properly.
     *
     * @throws Exception
     */
    @Test(dataProvider = "testSystemsProvider")
    public void testRenameManyObjects2(TSystem testSystem) throws Exception {
        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);

        addTestFilesToBucket(testSystem, "test1.txt", 10 * 1024);
        addTestFilesToBucket(testSystem, "test2.txt", 10 * 1024);
        addTestFilesToBucket(testSystem, "dir1/1.txt", 10 * 1024);
        addTestFilesToBucket(testSystem, "dir1/dir2/2.txt", 10 * 1024);
        addTestFilesToBucket(testSystem, "dir1/dir2/dir3/3.txt", 10 * 1024);
        addTestFilesToBucket(testSystem, "dir1/dir2/dir3/dir4.txt", 10 * 1024);

        MoveCopyRenameRequest request = new MoveCopyRenameRequest();
        request.setOperation(MoveCopyRenameOperation.RENAME);
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




    @Test(dataProvider = "testSystemsProvider")
    public void testDeleteManyObjects(TSystem testSystem) throws Exception {
        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
        addTestFilesToBucket(testSystem, "test1.txt", 10 * 1024);
        addTestFilesToBucket(testSystem, "test2.txt", 10 * 1024);
        addTestFilesToBucket(testSystem, "dir1/1.txt", 10 * 1024);
        addTestFilesToBucket(testSystem, "dir1/dir2/2.txt", 10 * 1024);
        addTestFilesToBucket(testSystem, "dir1/dir2/dir3/3.txt", 10 * 1024);
        addTestFilesToBucket(testSystem, "dir1/dir2/dir3/dir4.txt", 10 * 1024);

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
    public void testInsertFile(TSystem testSystem) throws Exception {
        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
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

    @Test(dataProvider = "testSystemsProvider")
    public void testMkdir(TSystem testSystem) throws Exception {
        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);

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
        Assert.assertEquals(listing.getResult().get(0).getType(), "dir");
    }



    @Test(dataProvider = "testSystemsProvider")
    public void testMkdirNoSlash(TSystem testSystem) throws Exception {
        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
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
        Assert.assertEquals(listing.getResult().get(0).getType(), "dir");
    }


    public String[] mkdirData() {
        return new String[]{
            "//newDirectory///test",
            "newDirectory///test/",
            "newDirectory/test"
        };
    }

    @DataProvider(name="mkdirDataProvider")
    public Object[] mkdirDataProvider() {
        List<String> directories = Arrays.asList(mkdirData());
        List<Pair<String, TSystem>> out = testSystems.stream().flatMap(sys -> directories.stream().map(j -> Pair.of(j, sys)))
            .collect(Collectors.toList());
        return out.toArray();

    }


    @Test(dataProvider = "mkdirDataProvider")
    public void testMkdirWithSlashes(Pair<String, TSystem> inputs) throws Exception {
        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(inputs.getRight());

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
        List<Pair<String, TSystem>> out = testSystems.stream().flatMap(sys -> directories.stream().map(j -> Pair.of(j, sys)))
            .collect(Collectors.toList());
        return out.toArray();
    }


    /**
     * All the funky paths should get cleaned up
     * @param inputs
     * @throws Exception
     */
    @Test(dataProvider = "mkdirBadDataProvider")
    public void testMkdirWithBadData(Pair<String, TSystem> inputs) throws Exception {
        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(inputs.getRight());
        MkdirRequest req = new MkdirRequest();
        req.setPath(inputs.getLeft());
        Response response = target("/v3/files/ops/testSystem/")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .post(Entity.json(req), Response.class);

        Assert.assertEquals(response.getStatus(), 400);
    }

}