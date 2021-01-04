package edu.utexas.tacc.tapis.files.api.resources;


import edu.utexas.tacc.tapis.files.api.BaseResourceConfig;
import edu.utexas.tacc.tapis.files.lib.caches.FilePermsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.shared.ssh.SSHConnectionCache;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.clients.S3DataClient;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.sharedapi.jaxrs.filters.JWTValidateRequestFilter;
import edu.utexas.tacc.tapis.shared.security.ServiceJWT;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Site;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;
import org.apache.commons.codec.Charsets;
import org.apache.commons.io.IOUtils;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.*;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@Test(groups = {"integration"})
public class ITestContentsRoutesS3 extends BaseDatabaseIntegrationTest {

    private Logger log = LoggerFactory.getLogger(ITestContentsRoutesS3.class);
    private String user1jwt;
    private String user2jwt;
    private TSystem testSystem;
    private Tenant tenant;
    private Credential creds;
    private Map<String, Tenant> tenantMap = new HashMap<>();
    Site site;

    // mocking out the services test
    private SystemsClient systemsClient;
    private SKClient skClient;
    private TenantManager tenantManager;
    private ServiceJWT serviceJWT;

    private ITestContentsRoutesS3() throws Exception {
        //List<String> creds = new ArrayList<>();
        creds = new Credential();
        creds.setAccessKey("user");
        creds.setAccessSecret("password");
        testSystem = new TSystem();
        testSystem.setHost("http://localhost");
        testSystem.setPort(9000);
        testSystem.setBucketName("test");
        testSystem.setId("testSystem");
        testSystem.setAuthnCredential(creds);
        testSystem.setRootDir("/");
        List<TSystem.TransferMethodsEnum> transferMechs = new ArrayList<>();
        transferMechs.add(TSystem.TransferMethodsEnum.S3);
        testSystem.setTransferMethods(transferMechs);

        tenant = new Tenant();
        tenant.setTenantId("testTenant");
        tenant.setBaseUrl("https://test.tapis.io");
        tenantMap.put(tenant.getTenantId(), tenant);
        site = new Site();
        site.setSiteId("dev");

    }

    @Override
    protected ResourceConfig configure() {
        enable(TestProperties.LOG_TRAFFIC);
        enable(TestProperties.DUMP_ENTITY);
        tenantManager = Mockito.mock(TenantManager.class);
        skClient = Mockito.mock(SKClient.class);
        systemsClient = Mockito.mock(SystemsClient.class);
        serviceJWT = Mockito.mock(ServiceJWT.class);
        JWTValidateRequestFilter.setService("files");
        JWTValidateRequestFilter.setSiteId("dev");
        ResourceConfig app = new BaseResourceConfig()
            .register(new JWTValidateRequestFilter(tenantManager))
            .register(new AbstractBinder() {
                @Override
                protected void configure() {
                    bind(systemsClient).to(SystemsClient.class);
                    bind(skClient).to(SKClient.class);
                    bind(tenantManager).to(TenantManager.class);
                    bind(serviceJWT).to(ServiceJWT.class);
                    bindAsContract(SystemsCache.class);
                    bindAsContract(FilePermsCache.class);
                    bindAsContract(RemoteDataClientFactory.class);
                    bind(new SSHConnectionCache(1, TimeUnit.MINUTES)).to(SSHConnectionCache.class);
                }
            });

        app.register(ContentApiResource.class);
        return app;
    }

    public class RandomInputStream extends InputStream {

        private long length;
        private long count;
        private Random random;

        public RandomInputStream(long length) {
            this.length = length;
            this.random = new Random();
        }

        @Override
        public int read() throws IOException {
            if (count >= length) {
                return -1;
            }
            count++;
            return random.nextInt();
        }
    }

    private InputStream makeFakeFile(long size){

        InputStream is = new RandomInputStream(size);
        return is;
    }

    private void addTestFilesToBucket(TSystem system, String fileName, long fileSize) throws Exception{
        S3DataClient client = new S3DataClient(system);
        InputStream f1 = makeFakeFile(fileSize);
        client.insert(fileName, f1);
    }


    @AfterClass
    public void tearDownTest() throws Exception {
        S3DataClient client = new S3DataClient(testSystem);
        client.delete("/");
    }

    @BeforeMethod
    public void beforeTest() throws Exception {
        when(tenantManager.getTenants()).thenReturn(tenantMap);
        when(tenantManager.getTenant(any())).thenReturn(tenant);
        when(tenantManager.getSite(any())).thenReturn(site);
        when(systemsClient.getUserCredential(any(), any())).thenReturn(creds);
        when(skClient.isPermitted(any(), any(String.class), any(String.class))).thenReturn(true);
        when(systemsClient.getSystemWithCredentials(any(String.class), any())).thenReturn(testSystem);
        user1jwt = IOUtils.resourceToString("/user1jwt", Charsets.UTF_8);
        user2jwt = IOUtils.resourceToString("/user2jwt", Charsets.UTF_8);
    }

    @BeforeClass
    @Override
    public void setUp() throws Exception {
        super.setUp();
    }


    @Test
    public void testStreamLargeFile() throws Exception {
        long filesize = 100 * 1000 * 1000;
        addTestFilesToBucket(testSystem, "testfile1.txt", filesize);
        Response response = target("/v3/files/content/testSystem/testfile1.txt")
            .request()
            .header("X-Tapis-Token", user1jwt)
            .get();
        InputStream contents = response.readEntity(InputStream.class);
        Instant start = Instant.now();
        long fsize=0;
        long chunk=0;
        byte[] buffer = new byte[1024];
        while((chunk = contents.read(buffer)) != -1){
            fsize += chunk;
        }
        Duration time = Duration.between(start, Instant.now());
        log.info("fileize={} MB: Download took {} ms: throughput={} MB/s", filesize /1E6, time.toMillis(), filesize / (1E6 * time.toMillis()/1000));
        Assert.assertEquals(fsize, filesize);
        contents.close();
    }


    @Test
    public void testSimpleGetContents() throws Exception {
        addTestFilesToBucket(testSystem, "testfile1.txt", 10*1024);
        Response response = target("/v3/files/content/testSystem/testfile1.txt")
                .request()
                .header("X-Tapis-Token", user1jwt)
                .get();
        byte[] contents = response.readEntity(byte[].class);
        Assert.assertEquals(contents.length, 10*1024);
    }

    @Test
    public void testNotFound() throws Exception {

        Response response = target("/v3/files/content/testSystem/NOT-THERE.txt")
                .request()
                .header("X-Tapis-Token", user1jwt)
                .get();
        Assert.assertEquals(response.getStatus(), 404);
    }

    @Test
    public void testGetWithRange() throws Exception {
        addTestFilesToBucket(testSystem, "words.txt", 10*1024);

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

        Response response = target("/v3/files/content/testSystem/BAD-PATH/")
                .request()
                .header("x-tapis-token", user1jwt)
                .get();
        Assert.assertEquals(response.getStatus(), 400);
    }

    //TODO: Add tests for strange chars in filename or path.


}
