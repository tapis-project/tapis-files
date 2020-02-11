package edu.utexas.tacc.tapis.files.api;


import edu.utexas.tacc.tapis.files.api.factories.FileOpsServiceFactory;
import edu.utexas.tacc.tapis.files.api.resources.ContentApiResource;
import edu.utexas.tacc.tapis.files.api.resources.OperationsApiResource;
import edu.utexas.tacc.tapis.files.lib.clients.S3DataClient;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import org.apache.commons.codec.Charsets;
import org.apache.commons.io.IOUtils;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.process.internal.RequestScoped;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTestNg;
import org.glassfish.jersey.test.TestProperties;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@Test(groups = {"integration"})
public class ITestContentsRoutesS3 extends JerseyTestNg.ContainerPerClassTest {

    private Logger log = LoggerFactory.getLogger(ITestContentsRoutesS3.class);
    private String user1jwt;
    private String user2jwt;
    private TSystem testSystem;

    // mocking out the services
    private SystemsClient systemsClient = Mockito.mock(SystemsClient.class);
    private SKClient skClient = Mockito.mock(SKClient.class);

    private ITestContentsRoutesS3() {
        //List<String> creds = new ArrayList<>();
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
    }

    @Override
    protected ResourceConfig configure() {
        enable(TestProperties.LOG_TRAFFIC);
        enable(TestProperties.DUMP_ENTITY);
        ResourceConfig app = new BaseResourceConfig()
                .register(new AbstractBinder() {
                    @Override
                    protected void configure() {
                        bind(systemsClient).to(SystemsClient.class);
                        bind(skClient).to(SKClient.class);
                        bindFactory(FileOpsServiceFactory.class).to(FileOpsService.class).in(RequestScoped.class);

                    }
                });
        app.register(ContentApiResource.class);
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
    public void testSimpleGetContents() throws Exception {
        addTestFilesToBucket(testSystem, "testfile1.txt", 10*1024);
        when(systemsClient.getSystemByName(any(String.class), any(Boolean.class))).thenReturn(testSystem);
        Response response = target("/content/testSystem/testfile1.txt")
                .request()
                .header("X-Tapis-Token", user1jwt)
                .get();
        byte[] contents = response.readEntity(byte[].class);
        Assert.assertEquals(contents.length, 10*1024);
    }

    @Test
    public void testNotFound() throws Exception {
        when(systemsClient.getSystemByName(any(String.class), any(Boolean.class))).thenReturn(testSystem);
        Response response = target("/content/testSystem/NOT-THERE.txt")
                .request()
                .header("X-Tapis-Token", user1jwt)
                .get();
        Assert.assertEquals(response.getStatus(), 404);
    }

    @Test
    public void testGetContentsHeaders() throws Exception {
        // make sure content-type is application/octet-stream and filename is correct
        addTestFilesToBucket(testSystem, "testfile1.txt", 10*1024);
        when(systemsClient.getSystemByName(any(String.class), any(Boolean.class))).thenReturn(testSystem);
        Response response = target("/content/testSystem/testfile1.txt")
                .request()
                .header("x-tapis-token", user1jwt)
                .get();
        MultivaluedMap<String, Object> headers = response.getHeaders();
        String contentDisposition = (String) headers.getFirst("content-disposition");
        Assert.assertEquals(contentDisposition, "attachment; filename=testfile1.txt");
    }

    @Test
    public void testBadRequest() throws Exception {
        when(systemsClient.getSystemByName(any(String.class), any(Boolean.class))).thenReturn(testSystem);
        Response response = target("/content/testSystem/BAD-PATH/")
                .request()
                .header("x-tapis-token", user1jwt)
                .get();
        Assert.assertEquals(response.getStatus(), 400);
    }

    //TODO: Add tests for strange chars in filename or path.


}
