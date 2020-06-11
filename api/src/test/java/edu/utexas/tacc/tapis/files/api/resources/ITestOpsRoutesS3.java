package edu.utexas.tacc.tapis.files.api.resources;


import edu.utexas.tacc.tapis.files.api.BaseResourceConfig;
import edu.utexas.tacc.tapis.files.api.models.CreateDirectoryRequest;
import edu.utexas.tacc.tapis.files.lib.cache.SSHConnectionCache;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.sharedapi.jaxrs.filters.JWTValidateRequestFilter;
import edu.utexas.tacc.tapis.sharedapi.responses.TapisResponse;
import edu.utexas.tacc.tapis.files.lib.clients.S3DataClient;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.sharedapi.security.ServiceJWT;
import edu.utexas.tacc.tapis.sharedapi.security.TenantManager;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;
import org.apache.commons.codec.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.http.entity.ContentType;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;
import org.glassfish.jersey.process.internal.RequestScoped;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTestNg;
import org.glassfish.jersey.test.TestProperties;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.*;

import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@Test(groups = {"integration"})
public class ITestOpsRoutesS3 extends JerseyTestNg.ContainerPerClassTest {

    private Logger log = LoggerFactory.getLogger(ITestOpsRoutesS3.class);
    private String user1jwt;
    private String user2jwt;
    private static class FileListResponse extends TapisResponse<List<FileInfo>> {}
    private static class FileStringResponse extends TapisResponse<String>{}
    private TSystem testSystem;
    private Tenant tenant;

    // mocking out the services
    private SystemsClient systemsClient;
    private SKClient skClient;
    private TenantManager tenantManager;
    private ServiceJWT serviceJWT;

    private ITestOpsRoutesS3() throws Exception {
        Credential creds = new Credential();
        creds.setAccessKey("user");
        creds.setAccessSecret("password");
        testSystem = new TSystem();
        testSystem.setHost("http://localhost");
        testSystem.setPort(9000);
        testSystem.setBucketName("test");
        testSystem.setName("testSystem");
        testSystem.setAccessCredential(creds);
        testSystem.setRootDir("/");
        List<TSystem.TransferMethodsEnum> transferMechs = new ArrayList<>();
        transferMechs.add(TSystem.TransferMethodsEnum.S3);
        testSystem.setTransferMethods(transferMechs);

        tenant = new Tenant();
        tenant.setTenantId("testTenant");
        tenant.setBaseUrl("https://test.tapis.io");
        Map<String, Tenant> tenantMap = new HashMap<>();
        tenantMap.put(tenant.getTenantId(), tenant);
        when(tenantManager.getTenants()).thenReturn(tenantMap);
        when(tenantManager.getTenant(any())).thenReturn(tenant);
    }

    @Override
    protected ResourceConfig configure() {
        enable(TestProperties.LOG_TRAFFIC);
        enable(TestProperties.DUMP_ENTITY);
        tenantManager = Mockito.mock(TenantManager.class);
        skClient = Mockito.mock(SKClient.class);
        systemsClient = Mockito.mock(SystemsClient.class);
        serviceJWT = Mockito.mock(ServiceJWT.class);

        ResourceConfig app = new BaseResourceConfig()
                .register(new JWTValidateRequestFilter(tenantManager))
                .register(new AbstractBinder() {
                    @Override
                    protected void configure() {
                        bind(systemsClient).to(SystemsClient.class);
                        bind(skClient).to(SKClient.class);
                        bind(tenantManager).to(TenantManager.class);
                        bind(serviceJWT).to(ServiceJWT.class);
                        bindAsContract(RemoteDataClientFactory.class);
                        bind(new SSHConnectionCache(1, TimeUnit.MINUTES)).to(SSHConnectionCache.class);
                    }
                });

        app.register(OperationsApiResource.class);
        return app;
    }

    // Needed for the test client to be able to use Mutlipart/form posts;
    @Override
    protected void configureClient(ClientConfig config) {
        config.register(MultiPartFeature.class);
    }

    private InputStream makeFakeFile(int size){
        byte[] b = new byte[size];
        new Random().nextBytes(b);
        InputStream is = new ByteArrayInputStream(b);
        return is;
    }

    private void addTestFilesToBucket(TSystem system, String fileName, int fileSize) throws Exception{
        S3DataClient client = new S3DataClient(system);
        InputStream f1 = makeFakeFile(fileSize);
        client.insert(fileName, f1);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        S3DataClient client = new S3DataClient(testSystem);
        client.delete("/");

    }

    @BeforeClass
    public void setUp() throws Exception {
        super.setUp();
        user1jwt = IOUtils.resourceToString("/user1jwt", Charsets.UTF_8);
        user2jwt = IOUtils.resourceToString("/user2jwt", Charsets.UTF_8);
    }


    @Test
    public void  testGetS3List() throws Exception {
        addTestFilesToBucket(testSystem, "testfile1.txt", 10*1024);
        when(systemsClient.getSystemByName(any(String.class), any())).thenReturn(testSystem);
        when(skClient.isPermitted(any(), any(String.class), any(String.class))).thenReturn(true);

        FileListResponse response = target("/v3/files/ops/testSystem/")
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .header("x-tapis-token", user1jwt)
                .get(FileListResponse.class);
        FileInfo file = response.getResult().get(0);
        Assert.assertEquals(file.getName(), "testfile1.txt");
        Assert.assertEquals(file.getSize(), 10 * 1024);
    }

    @Test
    public void  testGetS3ListWithLimitAndOffset() throws Exception {
        addTestFilesToBucket(testSystem, "testfile1.txt", 10*1024);
        addTestFilesToBucket(testSystem, "testfile2.txt", 10*1024);
        addTestFilesToBucket(testSystem, "testfile3.txt", 10*1024);
        addTestFilesToBucket(testSystem, "testfile4.txt", 10*1024);
        addTestFilesToBucket(testSystem, "testfile5.txt", 10*1024);
        addTestFilesToBucket(testSystem, "testfile6.txt", 10*1024);
        addTestFilesToBucket(testSystem, "testfile7.txt", 10*1024);
        addTestFilesToBucket(testSystem, "testfile8.txt", 10*1024);

        when(skClient.isPermitted(any(), any(String.class), any(String.class))).thenReturn(true);
        when(systemsClient.getSystemByName(any(String.class), any())).thenReturn(testSystem);

        FileListResponse response = target("/v3/files/ops/testSystem/")
                .queryParam("limit", "2")
                .queryParam("offset", "2")
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .header("x-tapis-token", user1jwt)
                .get(FileListResponse.class);
        List<FileInfo> listing = response.getResult();
        Assert.assertEquals(listing.size(), 2);
        Assert.assertEquals(listing.get(0).getName(), "testfile3.txt");
    }


    @Test
    public void  testGetS3ListNoAuthz() throws Exception {
        addTestFilesToBucket(testSystem, "testfile1.txt", 10*1024);
        when(skClient.isPermitted(any(), any(String.class), any(String.class))).thenReturn(false);
        when(systemsClient.getSystemByName(any(String.class), any())).thenReturn(testSystem);

        Response response = target("/v3/files/ops/testSystem/testfile1.txt")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("x-tapis-token", user1jwt)
            .get();
        Assert.assertEquals(response.getStatus(), 403);
    }

    @Test
    public void testDelete() throws Exception {
        S3DataClient client = new S3DataClient(testSystem);
        addTestFilesToBucket(testSystem, "testfile1.txt", 10*1024);
        addTestFilesToBucket(testSystem, "testfile2.txt", 10*1024);
        addTestFilesToBucket(testSystem, "dir1/testfile3.txt", 10*1024);
        when(systemsClient.getSystemByName(any(String.class), any())).thenReturn(testSystem);
        when(skClient.isPermitted(any(), any(String.class), any(String.class))).thenReturn(true);
        FileStringResponse response = target("/v3/files/ops/testSystem/dir1/testfile3.txt")
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .header("x-tapis-token", user1jwt)
                .delete(FileStringResponse.class);

        Assert.assertThrows(NotFoundException.class, ()-> {
            client.ls("a/b/c/test.txt");
        });
        List<FileInfo> l2 = client.ls("/");
        Assert.assertTrue(l2.size() > 0);

    }

    @Test
    public void testRenameFile() throws Exception {
        S3DataClient client = new S3DataClient(testSystem);
        addTestFilesToBucket(testSystem, "testfile1.txt", 10*1024);
        addTestFilesToBucket(testSystem, "testfile2.txt", 10*1024);
        addTestFilesToBucket(testSystem, "dir1/testfile3.txt", 10*1024);
        when(systemsClient.getSystemByName(any(String.class), any())).thenReturn(testSystem);
        when(skClient.isPermitted(any(), any(String.class), any(String.class))).thenReturn(true);
        FileStringResponse response = target("/v3/files/ops/testSystem/dir1/testfile3.txt")
                .queryParam("newName", "renamed")
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .header("x-tapis-token", user1jwt)
                .put(Entity.entity("", MediaType.TEXT_PLAIN), FileStringResponse.class);
        //TODO: Add asserts
        Assert.assertThrows(NotFoundException.class, ()-> {
            client.ls("/a/b/c/test.txt");
        });
    }

    @Test
    public void testRenameManyObjects1() throws Exception{
        S3DataClient client = new S3DataClient(testSystem);
        addTestFilesToBucket(testSystem, "test1.txt", 10*1024);
        addTestFilesToBucket(testSystem, "test2.txt", 10*1024);
        addTestFilesToBucket(testSystem, "dir1/1.txt", 10*1024);
        addTestFilesToBucket(testSystem, "dir1/dir2/2.txt", 10*1024);
        addTestFilesToBucket(testSystem, "dir1/dir2/dir3/3.txt", 10*1024);
        addTestFilesToBucket(testSystem, "dir1/dir2/dir3/dir4.txt", 10*1024);

        when(systemsClient.getSystemByName(any(String.class), any())).thenReturn(testSystem);
        when(skClient.isPermitted(any(), any(String.class), any(String.class))).thenReturn(true);

        FileStringResponse response = target("/v3/files/ops/testSystem/dir1/")
                .queryParam("newName", "renamed/")
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .header("x-tapis-token", user1jwt)
                .put(Entity.entity("", MediaType.TEXT_PLAIN), FileStringResponse.class);
        //TODO: Add asserts
        Assert.assertThrows(NotFoundException.class, ()-> {
            client.ls("/a/b/c/test.txt");
        });
    }


    /**
     * Tests of objects deeper in the tree are renamed properly.
     * @throws Exception
     */
    @Test
    public void testRenameManyObjects2() throws Exception{
        S3DataClient client = new S3DataClient(testSystem);
        addTestFilesToBucket(testSystem, "test1.txt", 10*1024);
        addTestFilesToBucket(testSystem, "test2.txt", 10*1024);
        addTestFilesToBucket(testSystem, "dir1/1.txt", 10*1024);
        addTestFilesToBucket(testSystem, "dir1/dir2/2.txt", 10*1024);
        addTestFilesToBucket(testSystem, "dir1/dir2/dir3/3.txt", 10*1024);
        addTestFilesToBucket(testSystem, "dir1/dir2/dir3/dir4.txt", 10*1024);

        when(systemsClient.getSystemByName(any(String.class), any())).thenReturn(testSystem);
        when(skClient.isPermitted(any(), any(String.class), any(String.class))).thenReturn(true);

        FileStringResponse response = target("/v3/files/ops/testSystem/dir1/dir2/")
                .queryParam("newName", "renamed")
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .header("x-tapis-token", user1jwt)
                .put(Entity.entity("", MediaType.TEXT_PLAIN), FileStringResponse.class);
        //TODO: Add asserts
        List<FileInfo> listing = client.ls("dir1/1.txt");
        Assert.assertEquals(listing.size(), 1);
        listing = client.ls("dir1/renamed/2.txt");
        Assert.assertEquals(listing.size(), 1);
        Assert.assertThrows(NotFoundException.class, ()-> {
            client.ls("/a/b/c/test.txt");
        });
    }


    @Test
    public void testDeleteManyObjects(){

    }

    @Test
    public void testInsertFile() throws Exception {
        when(systemsClient.getSystemByName(any(String.class), any())).thenReturn(testSystem);
        when(skClient.isPermitted(any(), any(String.class), any(String.class))).thenReturn(true);

        InputStream inputStream = makeFakeFile(10*1024);
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
                .header("x-tapis-token", user1jwt)
                .post(Entity.entity(multiPart, MediaType.MULTIPART_FORM_DATA_TYPE), FileStringResponse.class);

        FileListResponse listing  = target("/v3/files/ops/testSystem/test-inserted.txt")
                .request()
                .header("x-tapis-token", user1jwt)
                .get(FileListResponse.class);
        Assert.assertTrue(listing.getResult().size() == 1);
    }

    @Test
    public void testMkdir() throws Exception {
        when(systemsClient.getSystemByName(any(String.class), any())).thenReturn(testSystem);
        when(skClient.isPermitted(any(), any(String.class), any(String.class))).thenReturn(true);

        FileStringResponse response = target("/v3/files/ops/testSystem/newDirectory/")
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .header("content-type", ContentType.TEXT_PLAIN)
                .header("x-tapis-token", user1jwt)
                .post(Entity.text(""), FileStringResponse.class);

        FileListResponse listing  = target("/v3/files/ops/testSystem/newDirectory")
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .header("x-tapis-token", user1jwt)
                .get(FileListResponse.class);

        Assert.assertEquals(listing.getResult().size(), 1);
        Assert.assertEquals(listing.getResult().get(0).getName(), "newDirectory/");
        Assert.assertTrue(listing.getResult().get(0).isDir());
    }

    @Test
    public void testMkdirNoSlash() throws Exception {
        when(systemsClient.getSystemByName(any(String.class), any())).thenReturn(testSystem);
        when(skClient.isPermitted(any(), any(String.class), any(String.class))).thenReturn(true);

        FileStringResponse response = target("/v3/files/ops/testSystem/newDirectory")
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .header("x-tapis-token", user1jwt)
                .post(Entity.text(""), FileStringResponse.class);

        FileListResponse listing  = target("/v3/files/ops/testSystem/newDirectory")
                .request()
                .header("x-tapis-token", user1jwt)
                .get(FileListResponse.class);

        Assert.assertEquals(listing.getResult().size(), 1);
        Assert.assertEquals(listing.getResult().get(0).getName(), "newDirectory/");
        Assert.assertEquals(listing.getResult().get(0).isDir(), true);
    }


    @DataProvider(name="mkdirDataProvider")
    public Object[] mkdirDataProvider () {
        return new String[]{
                "//newDirectory///test",
                "newDirectory///test/",
                "newDirectory/test"
        };
    }
    @Test(dataProvider = "mkdirDataProvider")
    public void testMkdirWithSlashes(String path) throws Exception {
        when(systemsClient.getSystemByName(any(String.class), any())).thenReturn(testSystem);
        when(skClient.isPermitted(any(), any(String.class), any(String.class))).thenReturn(true);

        FileStringResponse response = target("/v3/files/ops/testSystem/" + path)
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .header("x-tapis-token", user1jwt)
                .post(Entity.text(""), FileStringResponse.class);

        FileListResponse listing  = target("/v3/files/ops/testSystem/newDirectory/test")
                .request()
                .header("x-tapis-token", user1jwt)
                .get(FileListResponse.class);

        Assert.assertEquals(listing.getResult().size(),  1);
    }

    @DataProvider(name="mkdirBadDataProvider")
    public Object[] mkdirBadDataProvider () {
        return new String[]{
                "/newDirectory//../test",
                "newDirectory/../../test/",
                "../newDirectory/test",
                "newDirectory/.test"
        };
    }
    @Test(dataProvider = "mkdirBadDataProvider")
    public void testMkdirWithBadData(String path) throws Exception {
        when(systemsClient.getSystemByName(any(String.class), any())).thenReturn(testSystem);
        when(skClient.isPermitted(any(), any(String.class), any(String.class))).thenReturn(true);

        Response response = target("/v3/files/ops/testSystem/" + path)
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .header("x-tapis-token", user1jwt)
                .post(Entity.text(""), Response.class);

        Assert.assertEquals(response.getStatus(), 400);
    }

    //TODO: Add tests for strange chars in filename or path.


}
