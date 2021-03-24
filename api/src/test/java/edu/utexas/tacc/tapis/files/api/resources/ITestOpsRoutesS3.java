package edu.utexas.tacc.tapis.files.api.resources;


import edu.utexas.tacc.tapis.files.api.BaseResourceConfig;
import edu.utexas.tacc.tapis.files.api.models.MkdirRequest;
import edu.utexas.tacc.tapis.files.api.models.MoveCopyRenameOperation;
import edu.utexas.tacc.tapis.files.api.models.MoveCopyRenameRequest;
import edu.utexas.tacc.tapis.files.api.providers.FilePermissionsAuthz;
import edu.utexas.tacc.tapis.files.lib.caches.FilePermsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService;
import edu.utexas.tacc.tapis.files.lib.services.FilePermsService;
import edu.utexas.tacc.tapis.files.lib.services.IFileOpsService;
import edu.utexas.tacc.tapis.files.lib.utils.TenantCacheFactory;
import edu.utexas.tacc.tapis.shared.ssh.SSHConnectionCache;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.clients.S3DataClient;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.TransferMethodEnum;
import edu.utexas.tacc.tapis.sharedapi.jaxrs.filters.JWTValidateRequestFilter;
import edu.utexas.tacc.tapis.sharedapi.responses.TapisResponse;
import edu.utexas.tacc.tapis.shared.security.ServiceJWT;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Site;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;
import org.apache.commons.codec.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.http.entity.ContentType;
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
import org.testng.annotations.*;

import javax.inject.Singleton;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@Test(groups = {"integration"})
public class ITestOpsRoutesS3 extends BaseDatabaseIntegrationTest {

    private Logger log = LoggerFactory.getLogger(ITestOpsRoutesS3.class);
    private static class FileListResponse extends TapisResponse<List<FileInfo>> {
    }

    private static class FileStringResponse extends TapisResponse<String> {
    }

    private TSystem testSystem;
    private Credential creds;

    // mocking out the services
    private SystemsClient systemsClient;
    private SKClient skClient;
    private ServiceJWT serviceJWT;


    private ITestOpsRoutesS3() throws Exception {
        creds = new Credential();
        creds.setAccessKey("user");
        creds.setAccessSecret("password");
        testSystem = new TSystem();
        testSystem.setOwner("testuser1");
        testSystem.setHost("http://localhost");
        testSystem.setPort(9000);
        testSystem.setBucketName("test");
        testSystem.setId("testSystem");
        testSystem.setAuthnCredential(creds);
        testSystem.setRootDir("/");
        List<TransferMethodEnum> transferMechs = new ArrayList<>();
        transferMechs.add(TransferMethodEnum.S3);
        testSystem.setTransferMethods(transferMechs);
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

    // Needed for the test client to be able to use Mutlipart/form posts;
    @Override
    protected void configureClient(ClientConfig config) {
        config.register(MultiPartFeature.class);
   }

    @BeforeMethod
    public void initMocks() throws Exception {
        when(skClient.isPermitted(any(), any(), any())).thenReturn(true);
        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
    }


    @AfterMethod
    public void cleanup() throws Exception {
        S3DataClient client = new S3DataClient(testSystem);
        client.delete("/");

    }

    @Test
    public void testGetS3List() throws Exception {
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

    @Test
    public void testGetS3ListWithObo() throws Exception {
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
        S3DataClient client = new S3DataClient(system);
        InputStream f1 = makeFakeFile(fileSize);
        client.insert(fileName, f1);
    }




    @Test
    public void testGetS3ListWithLimitAndOffset() throws Exception {
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


    @Test
    public void testGetS3ListNoAuthz() throws Exception {
        addTestFilesToBucket(testSystem, "testfile1.txt", 10 * 1024);
        when(skClient.isPermitted(any(), any(String.class), any(String.class))).thenReturn(false);
        Response response = target("/v3/files/ops/testSystem/testfile1.txt")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .get();
        Assert.assertEquals(response.getStatus(), 403);
    }

    @Test
    public void testDelete() throws Exception {
        S3DataClient client = new S3DataClient(testSystem);
        addTestFilesToBucket(testSystem, "testfile1.txt", 10 * 1024);
        addTestFilesToBucket(testSystem, "testfile2.txt", 10 * 1024);
        addTestFilesToBucket(testSystem, "dir1/testfile3.txt", 10 * 1024);


        FileStringResponse response = target("/v3/files/ops/testSystem/dir1/testfile3.txt")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .delete(FileStringResponse.class);

        Assert.assertThrows(NotFoundException.class, () -> {
            client.ls("a/b/c/test.txt");
        });
        List<FileInfo> l2 = client.ls("/");
        Assert.assertTrue(l2.size() > 0);

    }

    @Test
    public void testRenameFile() throws Exception {
        S3DataClient client = new S3DataClient(testSystem);
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

        Assert.assertThrows(NotFoundException.class, () -> {
            client.ls("/a/b/c/test.txt");
        });

        List<FileInfo> listing = client.ls("/dir1/renamed");
        Assert.assertEquals(listing.size(), 1);
        Assert.assertEquals(listing.get(0).getPath(), "dir1/renamed");
    }

    @Test
    public void testRenameManyObjects1() throws Exception {
        S3DataClient client = new S3DataClient(testSystem);
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

        Assert.assertThrows(NotFoundException.class, () -> {
            client.ls("/dir1/1.txt");
        });
        Assert.assertTrue(client.ls("/renamed").size() > 0);
    }


    /**
     * Tests of objects deeper in the tree are renamed properly.
     *
     * @throws Exception
     */
    @Test
    public void testRenameManyObjects2() throws Exception {
        S3DataClient client = new S3DataClient(testSystem);
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

        List<FileInfo> listing = client.ls("dir1/1.txt");
        Assert.assertEquals(listing.size(), 1);
        listing = client.ls("dir1/renamed/2.txt");
        Assert.assertEquals(listing.size(), 1);
        Assert.assertThrows(NotFoundException.class, () -> {
            client.ls("/dir1/dir2/");
        });
    }


    @Test
    public void testDeleteManyObjects() throws Exception {
        S3DataClient client = new S3DataClient(testSystem);
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

        List<FileInfo> listing = client.ls("dir1/1.txt");
        Assert.assertEquals(listing.size(), 1);
        listing = client.ls("dir1/");
        Assert.assertEquals(listing.size(), 1);
        Assert.assertThrows(NotFoundException.class, () -> {
            client.ls("/dir1/dir2/");
        });
    }

    @Test
    public void testInsertFile() throws Exception {

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

    @Test
    public void testMkdir() throws Exception {

        MkdirRequest req = new MkdirRequest();
        req.setPath("newDirectory");
        
        FileStringResponse response = target("/v3/files/ops/testSystem/")
            .queryParam("path", "newDirectory")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .post(Entity.json(req), FileStringResponse.class);

        FileListResponse listing = target("/v3/files/ops/testSystem/newDirectory")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .get(FileListResponse.class);

        Assert.assertEquals(listing.getResult().size(), 1);
        Assert.assertEquals(listing.getResult().get(0).getName(), "newDirectory");
        Assert.assertTrue(listing.getResult().get(0).isDir());
    }



    @Test
    public void testMkdirNoSlash() throws Exception {
        MkdirRequest req = new MkdirRequest();
        req.setPath("newDirectory");

        FileStringResponse response = target("/v3/files/ops/testSystem/")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .post(Entity.json(req), FileStringResponse.class);

        FileListResponse listing = target("/v3/files/ops/testSystem/newDirectory")
            .request()
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .get(FileListResponse.class);

        Assert.assertEquals(listing.getResult().size(), 1);
        Assert.assertEquals(listing.getResult().get(0).getName(), "newDirectory");
        Assert.assertEquals(listing.getResult().get(0).isDir(), true);
    }


    @DataProvider(name = "mkdirDataProvider")
    public Object[] mkdirDataProvider() {
        return new String[]{
            "//newDirectory///test",
            "newDirectory///test/",
            "newDirectory/test"
        };
    }

    @Test(dataProvider = "mkdirDataProvider")
    public void testMkdirWithSlashes(String path) throws Exception {

        MkdirRequest req = new MkdirRequest();
        req.setPath(path);

        FileStringResponse response = target("/v3/files/ops/testSystem/")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .post(Entity.json(req), FileStringResponse.class);

        FileListResponse listing = target("/v3/files/ops/testSystem/newDirectory/test")
            .request()
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .get(FileListResponse.class);

        Assert.assertEquals(listing.getResult().size(), 1);
    }

    @DataProvider(name = "mkdirBadDataProvider")
    public Object[] mkdirBadDataProvider() {
        return new String[]{
            "/newDirectory//../test",
            "newDirectory/../../test/",
            "../newDirectory/test",
            "newDirectory/.test"
        };
    }

    @Test(dataProvider = "mkdirBadDataProvider")
    public void testMkdirWithBadData(String path) throws Exception {
        MkdirRequest req = new MkdirRequest();
        req.setPath(path);
        Response response = target("/v3/files/ops/testSystem/")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", getJwtForUser("dev", "testuser1"))
            .post(Entity.json(req), Response.class);

        Assert.assertEquals(response.getStatus(), 400);
    }

    //TODO: Add tests for strange chars in filename or path.


}
