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
import org.apache.commons.io.IOUtils;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTestNg;
import org.glassfish.jersey.test.TestProperties;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.AwsS3V4SignerParams;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;

import javax.ws.rs.core.MediaType;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@Test(groups = {"integration"})
public class ITestOpsRoutesS3 extends JerseyTestNg.ContainerPerClassTest {

    private Logger log = LoggerFactory.getLogger(ITestOpsRoutesS3.class);
    private String user1jwt;
    private String user2jwt;
    private static class FileListResponse extends TapisResponse<List<FileInfo>> {}
    private TSystem testSystem;

    // mocking out the systems service
    private FakeSystemsService systemsService = Mockito.mock(FakeSystemsService.class);

    private ITestOpsRoutesS3() {
        testSystem = new TSystem();
        testSystem.setHost("http://localhost");
        testSystem.setPort(9000);
        testSystem.setBucketName("test");
        testSystem.setName("testSystem");
        testSystem.setEffectiveUserId("user");
        testSystem.setAccessCredential("password");
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
        RequestBody body = RequestBody.create("<setCredsReq><username>test</username><password>password</password></setCredsReq>", okhttp3.MediaType.get("text/plain"));
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

    @BeforeClass
    private void setUpClass() throws Exception {
        user1jwt = IOUtils.resourceToString("/user1jwt", Charsets.UTF_8);
        user2jwt = IOUtils.resourceToString("/user2jwt", Charsets.UTF_8);
    }

    @BeforeTest
    private void beforeTest() throws Exception {

    }

    @AfterTest
    private void tearDownTest() throws Exception {

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

}
