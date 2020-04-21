package edu.utexas.tacc.tapis.files.api.resources;


import edu.utexas.tacc.tapis.files.api.BaseResourceConfig;
import edu.utexas.tacc.tapis.files.lib.clients.S3DataClient;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.sharedapi.jaxrs.filters.JWTValidateRequestFilter;
import edu.utexas.tacc.tapis.sharedapi.security.IServiceJWT;
import edu.utexas.tacc.tapis.sharedapi.security.ITenantManager;
import edu.utexas.tacc.tapis.sharedapi.security.ServiceJWT;
import edu.utexas.tacc.tapis.sharedapi.security.TenantManager;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;
import org.apache.commons.codec.Charsets;
import org.apache.commons.io.IOUtils;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTestNg;
import org.glassfish.jersey.test.TestProperties;
import org.mockito.Mock;
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
import java.util.*;

import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@Test(groups = {"integration"})
public class ITestContentsRoutesS3 extends JerseyTestNg.ContainerPerClassTest {

    private Logger log = LoggerFactory.getLogger(ITestContentsRoutesS3.class);
    private String user1jwt;
    private String user2jwt;
    private TSystem testSystem;
    private Tenant tenant;

    // mocking out the services
    private SystemsClient systemsClient;
    private SKClient skClient;
    private TenantManager tenantManager;
    private ServiceJWT serviceJWT;

    private ITestContentsRoutesS3() throws TapisException {
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
        when(systemsClient.getSystemByName(any(String.class))).thenReturn(testSystem);
        when(skClient.isPermitted(any(), any(String.class), any(String.class))).thenReturn(true);

        Response response = target("/v3/files/content/testSystem/testfile1.txt")
                .request()
                .header("X-Tapis-Token", user1jwt)
                .get();
        byte[] contents = response.readEntity(byte[].class);
        Assert.assertEquals(contents.length, 10*1024);
    }

    @Test
    public void testNotFound() throws Exception {
        when(systemsClient.getSystemByName(any(String.class))).thenReturn(testSystem);
        when(skClient.isPermitted(any(), any(String.class), any(String.class))).thenReturn(true);
        Response response = target("/v3/files/content/testSystem/NOT-THERE.txt")
                .request()
                .header("X-Tapis-Token", user1jwt)
                .get();
        Assert.assertEquals(response.getStatus(), 404);
    }

    @Test
    public void testGetWithRange() throws Exception {
        addTestFilesToBucket(testSystem, "words.txt", 10*1024);
        when(systemsClient.getSystemByName(any(String.class))).thenReturn(testSystem);
        when(skClient.isPermitted(any(), any(String.class), any(String.class))).thenReturn(true);
        Response response = target("/v3/files/content/testSystem/words.txt")
                .request()
                .header("range", "0,1000")
                .header("X-Tapis-Token", user1jwt)
                .get();
        Assert.assertEquals(response.getStatus(), 200);
        byte[] contents = response.readEntity(byte[].class);
        Assert.assertEquals(contents.length, 1000);
    }

    @Test
    public void testGetWithMore() throws Exception {
        addTestFilesToBucket(testSystem, "words.txt", 10*1024);
        when(systemsClient.getSystemByName(any(String.class))).thenReturn(testSystem);
        when(skClient.isPermitted(any(), any(String.class), any(String.class))).thenReturn(true);
        Response response = target("/v3/files/content/testSystem/words.txt")
                .request()
                .header("more", "1")
                .header("X-Tapis-Token", user1jwt)
                .get();
        Assert.assertEquals(response.getStatus(), 200);
        String contents = response.readEntity(String.class);
        System.out.println(contents);
        Assert.assertEquals(response.getHeaders().getFirst("content-disposition"), "inline");
        //TODO: Its hard to say how many chars should be in there, some UTF-8 chars are 1 byte, some are 4. Need to have a better fixture
        Assert.assertTrue(contents.length() > 0);
    }

    @Test
    public void testGetContentsHeaders() throws Exception {
        // make sure content-type is application/octet-stream and filename is correct
        addTestFilesToBucket(testSystem, "testfile1.txt", 10*1024);
        when(systemsClient.getSystemByName(any(String.class))).thenReturn(testSystem);
        when(skClient.isPermitted(any(), any(String.class), any(String.class))).thenReturn(true);
        Response response = target("/v3/files/content/testSystem/testfile1.txt")
                .request()
                .header("x-tapis-token", user1jwt)
                .get();
        MultivaluedMap<String, Object> headers = response.getHeaders();
        String contentDisposition = (String) headers.getFirst("content-disposition");
        Assert.assertEquals(contentDisposition, "attachment; filename=testfile1.txt");
    }

    @Test
    public void testBadRequest() throws Exception {
        when(systemsClient.getSystemByName(any(String.class))).thenReturn(testSystem);
        when(skClient.isPermitted(any(), any(String.class), any(String.class))).thenReturn(true);
        Response response = target("/v3/files/content/testSystem/BAD-PATH/")
                .request()
                .header("x-tapis-token", user1jwt)
                .get();
        Assert.assertEquals(response.getStatus(), 400);
    }

    //TODO: Add tests for strange chars in filename or path.


}
