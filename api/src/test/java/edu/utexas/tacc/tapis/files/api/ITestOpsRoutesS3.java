package edu.utexas.tacc.tapis.files.api;


import edu.utexas.tacc.tapis.files.api.resources.OperationsApiResource;
import edu.utexas.tacc.tapis.files.api.utils.TapisResponse;
import edu.utexas.tacc.tapis.files.lib.clients.FakeSystemsService;
import edu.utexas.tacc.tapis.files.lib.clients.S3DataClient;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.commons.codec.Charsets;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.io.IOUtils;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTestNg;
import org.glassfish.jersey.test.TestProperties;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.*;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.AwsS3V4SignerParams;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.*;

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

    // mocking out the systems service
    private FakeSystemsService systemsService = Mockito.mock(FakeSystemsService.class);

    private ITestOpsRoutesS3() {
        List<String> creds = new ArrayList<>();
        creds.add("password");
        testSystem = new TSystem();
        testSystem.setHost("http://localhost");
        testSystem.setPort(9000);
        testSystem.setBucketName("test");
        testSystem.setName("testSystem");
        testSystem.setEffectiveUserId("user");
        testSystem.setAccessCredential(creds);
        testSystem.setRootDir("/");
        List<TSystem.TransferMechanismsEnum> transferMechs = new ArrayList<>();
        transferMechs.add(TSystem.TransferMechanismsEnum.S3);
        testSystem.setTransferMechanisms(transferMechs);
    }

    @Override
    protected ResourceConfig configure() {
        enable(TestProperties.LOG_TRAFFIC);
        enable(TestProperties.DUMP_ENTITY);
        ResourceConfig app = new BaseResourceConfig()
                .register(OperationsApiResource.class)
                .register(new AbstractBinder() {
                    @Override
                    protected void configure() {
                        bind(systemsService).to(FakeSystemsService.class);
                    }
                });
        return app;
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


    private void createTestUserForMinio() throws Exception {
        SdkHttpFullRequest awsDummyRequest = SdkHttpFullRequest.builder()
                .method(SdkHttpMethod.GET)
                .uri(new URI("http://localhost:9000/?system"))
                .build();
        Aws4Signer signer = Aws4Signer.create();
        AwsS3V4SignerParams signerParams = AwsS3V4SignerParams.builder()
                .signingName("s3")
                .signingRegion(Region.US_EAST_1)
                .awsCredentials(AwsBasicCredentials.create("test", "password"))
                .build();
        awsDummyRequest = signer.sign(awsDummyRequest, signerParams);

        OkHttpClient client = new OkHttpClient();
        RequestBody body = RequestBody.create("<setCredsReq><username>test</username><password>password</password></setCredsReq>", okhttp3.MediaType.get("text/xml"));
        Request.Builder builder = new Request.Builder();
        builder.url("http://localhost:9000/minio/admin/v2/add-user")
                .put(body);
        for (Map.Entry<String, List<String>> header : awsDummyRequest.headers().entrySet()) {
            for(String entry: header.getValue()) {
                builder.header(header.getKey(), entry);
            }

        }
        builder.header("x-minio-operation", "creds");
        Request request = builder.build();
        Response response = client.newCall(request).execute();

    }

    @AfterClass
    public void tearDown() throws Exception {
        super.tearDown();
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
        when(systemsService.getSystemByName(any())).thenReturn(testSystem);
        FileListResponse response = target("/ops/testSystem/test")
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .header("x-tapis-token", user1jwt)
                .get(FileListResponse.class);
        FileInfo file = response.getResult().get(0);
        Assert.assertEquals(file.getName(), "testfile1.txt");
        Assert.assertEquals(file.getSize(), Long.valueOf(10 * 1024));
    }

    @Test
    public void testDelete() throws Exception {
        S3DataClient client = new S3DataClient(testSystem);
        addTestFilesToBucket(testSystem, "testfile1.txt", 10*1024);
        addTestFilesToBucket(testSystem, "testfile2.txt", 10*1024);
        addTestFilesToBucket(testSystem, "dir1/testfile3.txt", 10*1024);
        when(systemsService.getSystemByName(any())).thenReturn(testSystem);
        FileStringResponse response = target("/ops/testSystem/dir1/testfile3.txt")
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .header("x-tapis-token", user1jwt)
                .delete(FileStringResponse.class);
        //TODO: Add asserts
        List<FileInfo> listing = client.ls("dir1/testfile3.txt");
        Assert.assertEquals(listing.size(), 0);
        List<FileInfo> l2 = client.ls("/");
        Assert.assertTrue(l2.size() > 0);

    }

    @Test
    public void testRenameFile() throws Exception {
        S3DataClient client = new S3DataClient(testSystem);
        addTestFilesToBucket(testSystem, "testfile1.txt", 10*1024);
        addTestFilesToBucket(testSystem, "testfile2.txt", 10*1024);
        addTestFilesToBucket(testSystem, "dir1/testfile3.txt", 10*1024);
        when(systemsService.getSystemByName(any())).thenReturn(testSystem);
        FileStringResponse response = target("/ops/testSystem/dir1/testfile3.txt")
                .queryParam("newName", "renamed")
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .header("x-tapis-token", user1jwt)
                .put(Entity.entity("", MediaType.TEXT_PLAIN), FileStringResponse.class);
        //TODO: Add asserts
        List<FileInfo> listing = client.ls("dir1/testfile3.txt");
        Assert.assertEquals(listing.size(), 0);
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

        when(systemsService.getSystemByName(any())).thenReturn(testSystem);
        FileStringResponse response = target("/ops/testSystem/dir1/")
                .queryParam("newName", "renamed/")
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .header("x-tapis-token", user1jwt)
                .put(Entity.entity("", MediaType.TEXT_PLAIN), FileStringResponse.class);
        //TODO: Add asserts
        List<FileInfo> listing = client.ls("dir1/");
        Assert.assertEquals(listing.size(), 0);
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

        when(systemsService.getSystemByName(any())).thenReturn(testSystem);
        FileStringResponse response = target("/ops/testSystem/dir1/dir2/")
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
        listing = client.ls("dir1/dir2/2.txt");
        Assert.assertEquals(listing.size(), 0);
    }


    @Test
    public void testDeleteManyObjects(){

    }

    //TODO: Add tests for strange chars in filename or path.


}
