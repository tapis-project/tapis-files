package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.net.URI;

@Test(groups = "integration")
public class S3DataClientTests extends BaseDataClientTests<S3DataClient> {
    private static final String JSON_TEST_PATH="edu/utexas/tacc/tapis/files/lib/clients/TestSystems.json";

    public S3DataClientTests() {
        super(S3DataClientTests.JSON_TEST_PATH);
    }

    @Test
    public void testHostNoScheme() throws Exception {
        TapisSystem sys = new TapisSystem();
        Credential creds = new Credential();
        creds.setAccessKey("testKey");
        creds.setAccessSecret("testSecret");
        sys.setHost("test.tacc.io");
        sys.setBucketName("testBucket");
        sys.setPort(9000);
        sys.setAuthnCredential(creds);

        S3DataClient client = new S3DataClient("oboTenant", "oboUser", sys);
        URI tmpURI = client.configEndpoint(sys.getHost(), sys.getPort());
        Assert.assertEquals(tmpURI.getScheme(), "https");
        Assert.assertEquals(tmpURI.toString(), "https://test.tacc.io:9000");
    }

    @Test
    public void testHostContainsHTTPScheme() throws Exception {
        TapisSystem sys = new TapisSystem();
        Credential creds = new Credential();
        creds.setAccessKey("testKey");
        creds.setAccessSecret("testSecret");
        sys.setHost("http://test.tacc.io");
        sys.setBucketName("testBucket");
        sys.setPort(9000);
        sys.setAuthnCredential(creds);

        S3DataClient client = new S3DataClient("oboTenant", "oboUser", sys);
        URI tmpURI = client.configEndpoint(sys.getHost(), sys.getPort());
        Assert.assertEquals(tmpURI.getScheme(), "http");
        Assert.assertEquals(tmpURI.toString(), "http://test.tacc.io:9000");
    }

    @Test
    public void testHostContainsHTTPSScheme() throws Exception {
        TapisSystem sys = new TapisSystem();
        Credential creds = new Credential();
        creds.setAccessKey("testKey");
        creds.setAccessSecret("testSecret");
        sys.setHost("https://test.tacc.io/bucket");
        sys.setBucketName("testBucket");
        sys.setPort(9000);
        sys.setAuthnCredential(creds);

        S3DataClient client = new S3DataClient("oboTenant", "oboUser", sys);
        URI tmpURI = client.configEndpoint(sys.getHost(),sys.getPort());
        Assert.assertEquals(tmpURI.getScheme(), "https");
        Assert.assertEquals(tmpURI.toString(), "https://test.tacc.io:9000");
    }

    @Override
    protected String getConfigSection() {
        return "s3_system";
    }

    @Override
    public S3DataClient createDataClient(String tenantName, String userName, TapisSystem system, SystemsCache systemsCache,
                                            String impersonationId, String sharedCtxGrantor) throws Exception {
        return new S3DataClient(tenantName, userName, system);
    }
}
