package edu.utexas.tacc.tapis.files.api.resources;


import edu.utexas.tacc.tapis.files.api.BaseResourceConfig;
import edu.utexas.tacc.tapis.files.lib.caches.FilePermsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.SSHDataClient;
import edu.utexas.tacc.tapis.files.lib.services.FilePermsService;
import edu.utexas.tacc.tapis.files.lib.utils.TenantCacheFactory;
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
import edu.utexas.tacc.tapis.systems.client.gen.model.TransferMethodEnum;
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

import javax.inject.Singleton;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@Test(groups = {"integration"})
public class ITestContentsRoutes extends BaseDatabaseIntegrationTest {

    private final Logger log = LoggerFactory.getLogger(ITestContentsRoutes.class);
    private final TSystem testSystem;
    private final TSystem testSystemSSH;
    private final Tenant tenant;
    private final Credential creds;
    private final Map<String, Tenant> tenantMap = new HashMap<>();
    Site site;

    // mocking out the services test
    private SystemsClient systemsClient;
    private SKClient skClient;
    private ServiceJWT serviceJWT;
    private final SSHConnectionCache sshConnectionCache = new SSHConnectionCache(1, TimeUnit.SECONDS);
    private final RemoteDataClientFactory remoteDataClientFactory = new RemoteDataClientFactory(sshConnectionCache);

    private ITestContentsRoutes() throws Exception {
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
        List<TransferMethodEnum> transferMechs = new ArrayList<>();
        transferMechs.add(TransferMethodEnum.S3);
        testSystem.setTransferMethods(transferMechs);


        //SSH system with username/password
        Credential sshCreds = new Credential();
        sshCreds.setAccessKey("testuser");
        sshCreds.setPassword("password");
        testSystemSSH = new TSystem();
        testSystemSSH.setAuthnCredential(sshCreds);
        testSystemSSH.setHost("localhost");
        testSystemSSH.setPort(2222);
        testSystemSSH.setRootDir("/data/home/testuser/");
        testSystemSSH.setId("destSystem");
        testSystemSSH.setEffectiveUserId("testuser");
        List<TransferMethodEnum> tMechs = new ArrayList<>();
        tMechs.add(TransferMethodEnum.SFTP);
        testSystemSSH.setTransferMethods(tMechs);

        tenant = new Tenant();
        tenant.setTenantId("testTenant");
        tenant.setBaseUrl("https://test.tapis.io");
        tenantMap.put(tenant.getTenantId(), tenant);
        site = new Site();
        site.setSiteId("tacc");

    }

    @Override
    protected ResourceConfig configure() {
        enable(TestProperties.LOG_TRAFFIC);
        enable(TestProperties.DUMP_ENTITY);
        skClient = Mockito.mock(SKClient.class);
        systemsClient = Mockito.mock(SystemsClient.class);
        serviceJWT = Mockito.mock(ServiceJWT.class);
        JWTValidateRequestFilter.setService("files");
        JWTValidateRequestFilter.setSiteId("tacc");
        ResourceConfig app = new BaseResourceConfig()
            .register(JWTValidateRequestFilter.class)
            .register(new AbstractBinder() {
                @Override
                protected void configure() {
                    bind(systemsClient).to(SystemsClient.class);
                    bind(skClient).to(SKClient.class);
                    bindFactory(TenantCacheFactory.class).to(TenantManager.class).in(Singleton.class);
                    bind(serviceJWT).to(ServiceJWT.class);
                    bindAsContract(SystemsCache.class);
                    bindAsContract(FilePermsService.class);
                    bindAsContract(FilePermsCache.class);
                    bindAsContract(RemoteDataClientFactory.class);
                    bind(sshConnectionCache).to(SSHConnectionCache.class);
                }
            });

        app.register(ContentApiResource.class);
        return app;
    }

    @BeforeClass
    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    public class RandomInputStream extends InputStream {

        private final long length;
        private long count;
        private final Random random;

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

    private InputStream makeFakeFile(long size) {

        InputStream is = new RandomInputStream(size);
        return is;
    }

    private void addTestFilesToBucket(TSystem system, String fileName, long fileSize) throws Exception {
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(system, "testuser");
        InputStream f1 = makeFakeFile(fileSize);
        client.insert(fileName, f1);
    }


    public void tearDownTest() throws Exception {
        S3DataClient client = new S3DataClient(testSystem);
        client.delete("/");

        IRemoteDataClient client2 = remoteDataClientFactory.getRemoteDataClient(testSystemSSH, "testuser");
        client2.delete("/");
    }

    @BeforeMethod
    public void beforeTest() throws Exception {
        when(skClient.isPermitted(any(), any(String.class), any(String.class))).thenReturn(true);
        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
    }


    @DataProvider(name = "testSystemsDataProvider")
    public Object[] mkdirDataProvider() {
        return new TSystem[]{testSystem, testSystemSSH};
    }


    @Test(dataProvider = "testSystemsDataProvider")
    public void testZipOutput(TSystem system) throws Exception {
        when(systemsClient.getSystemWithCredentials(any(String.class), any())).thenReturn(system);
        addTestFilesToBucket(system, "a/test1.txt", 10 * 1000);
        addTestFilesToBucket(system, "a/test2.txt", 10 * 1000);
        Response response = target("/v3/files/content/testSystem/a/")
            .queryParam("zip", true)
            .request()
            .header("X-Tapis-Token", getJwtForUser("dev", "testuser1"))
            .get();

        InputStream is = response.readEntity(InputStream.class);
        ZipInputStream zis = new ZipInputStream(is);
        ZipEntry ze;
        while ((ze = zis.getNextEntry()) != null) {
            log.info(ze.toString());
            String fname = ze.getName();
            Assert.assertTrue(fname.startsWith("a/test"));
        }
    }


    @Test(dataProvider = "testSystemsDataProvider")
    public void testStreamLargeFile(TSystem system) throws Exception {
        when(systemsClient.getSystemWithCredentials(any(String.class), any())).thenReturn(system);
        long filesize = 100 * 1000 * 1000;
        addTestFilesToBucket(system, "testfile1.txt", filesize);
        Response response = target("/v3/files/content/testSystem/testfile1.txt")
            .request()
            .header("X-Tapis-Token", getJwtForUser("dev", "testuser1"))
            .get();
        InputStream contents = response.readEntity(InputStream.class);
        Instant start = Instant.now();
        long fsize = 0;
        long chunk = 0;
        byte[] buffer = new byte[1024];
        while ((chunk = contents.read(buffer)) != -1) {
            fsize += chunk;
        }
        Duration time = Duration.between(start, Instant.now());
        log.info("file size={} MB: Download took {} ms: throughput={} MB/s", filesize / 1E6, time.toMillis(), filesize / (1E6 * time.toMillis() / 1000));
        Assert.assertEquals(fsize, filesize);
        contents.close();
    }


    @Test(dataProvider = "testSystemsDataProvider")
    public void testSimpleGetContents(TSystem system) throws Exception {
        when(systemsClient.getSystemWithCredentials(any(String.class), any())).thenReturn(system);
        addTestFilesToBucket(system, "testfile1.txt", 10 * 1000);
        Response response = target("/v3/files/content/testSystem/testfile1.txt")
            .request()
            .header("X-Tapis-Token", getJwtForUser("dev", "testuser1"))
            .get();
        byte[] contents = response.readEntity(byte[].class);
        Assert.assertEquals(contents.length, 10 * 1000);
    }

    @Test(dataProvider = "testSystemsDataProvider")
    public void testNotFound(TSystem system) throws Exception {
        when(systemsClient.getSystemWithCredentials(any(String.class), any())).thenReturn(system);
        Response response = target("/v3/files/content/testSystem/NOT-THERE.txt")
            .request()
            .header("X-Tapis-Token", getJwtForUser("dev", "testuser1"))
            .get();
        Assert.assertEquals(response.getStatus(), 404);
    }

    @Test(dataProvider = "testSystemsDataProvider")
    public void testGetWithRange(TSystem system) throws Exception {
        when(systemsClient.getSystemWithCredentials(any(String.class), any())).thenReturn(system);
        addTestFilesToBucket(system, "words.txt", 10 * 1024);
        Response response = target("/v3/files/content/testSystem/words.txt")
            .request()
            .header("range", "0,1000")
            .header("X-Tapis-Token", getJwtForUser("dev", "testuser1"))
            .get();
        Assert.assertEquals(response.getStatus(), 200);
        byte[] contents = response.readEntity(byte[].class);
        Assert.assertEquals(contents.length, 1000);
    }

    @Test(dataProvider = "testSystemsDataProvider")
    public void testGetWithMore(TSystem system) throws Exception {
        when(systemsClient.getSystemWithCredentials(any(String.class), any())).thenReturn(system);
        addTestFilesToBucket(system, "words.txt", 10 * 1024);
        Response response = target("/v3/files/content/testSystem/words.txt")
            .request()
            .header("more", "1")
            .header("X-Tapis-Token", getJwtForUser("dev", "testuser1"))
            .get();
        Assert.assertEquals(response.getStatus(), 200);
        String contents = response.readEntity(String.class);
        Assert.assertEquals(response.getHeaders().getFirst("content-disposition"), "inline");
        //TODO: Its hard to say how many chars should be in there, some UTF-8 chars are 1 byte, some are 4. Need to have a better fixture
        Assert.assertTrue(contents.length() > 0);
    }

    @Test(dataProvider = "testSystemsDataProvider")
    public void testGetContentsHeaders(TSystem system) throws Exception {
        when(systemsClient.getSystemWithCredentials(any(String.class), any())).thenReturn(system);
        // make sure content-type is application/octet-stream and filename is correct
        addTestFilesToBucket(system, "testfile1.txt", 10 * 1024);

        Response response = target("/v3/files/content/testSystem/testfile1.txt")
            .request()
            .header("X-Tapis-Token", getJwtForUser("dev", "testuser1"))
            .get();
        MultivaluedMap<String, Object> headers = response.getHeaders();
        String contentDisposition = (String) headers.getFirst("content-disposition");
        Assert.assertEquals(contentDisposition, "attachment; filename=testfile1.txt");
    }


    // Tries to serve a folder, which is not allowed resulting in 400
    @Test(dataProvider = "testSystemsDataProvider")
    public void testBadRequest(TSystem system) throws Exception {

        Response response = target("/v3/files/content/testSystem/BAD-PATH/")
            .request()
            .header("X-Tapis-Token", getJwtForUser("dev", "testuser1"))
            .get();
        Assert.assertEquals(response.getStatus(), 400);
    }

    //TODO: Add tests for strange chars in filename or path.


}
