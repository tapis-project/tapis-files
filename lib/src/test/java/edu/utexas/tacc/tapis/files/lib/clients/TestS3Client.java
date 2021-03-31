package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import org.testng.Assert;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;

@Test
public class TestS3Client {

    private final String oboTenant = "oboTenant";
    private final String oboUser = "oboUser";

    @Test
    public void testHostNoScheme() throws Exception {
        TSystem sys = new TSystem();
        Credential creds = new Credential();
        creds.setAccessKey("testKey");
        creds.setAccessSecret("testSecret");
        sys.setHost("test.tacc.io");
        sys.setBucketName("testBucket");
        sys.setPort(9000);
        sys.setAuthnCredential(creds);

        S3DataClient client = new S3DataClient(oboTenant, oboUser, sys);
        URI tmpURI = client.configEndpoint(sys.getHost());
        Assert.assertEquals(tmpURI.getScheme(), "https");
        Assert.assertEquals(tmpURI.toString(), "https://test.tacc.io:9000");
    }

    @Test
    public void testHostContainsHTTPScheme() throws Exception {
        TSystem sys = new TSystem();
        Credential creds = new Credential();
        creds.setAccessKey("testKey");
        creds.setAccessSecret("testSecret");
        sys.setHost("http://test.tacc.io");
        sys.setBucketName("testBucket");
        sys.setPort(9000);
        sys.setAuthnCredential(creds);

        S3DataClient client = new S3DataClient(oboTenant, oboUser, sys);
        URI tmpURI = client.configEndpoint(sys.getHost());
        Assert.assertEquals(tmpURI.getScheme(), "http");
        Assert.assertEquals(tmpURI.toString(), "http://test.tacc.io:9000");
    }

    @Test
    public void testHostContainsHTTPSScheme() throws Exception {
        TSystem sys = new TSystem();
        Credential creds = new Credential();
        creds.setAccessKey("testKey");
        creds.setAccessSecret("testSecret");
        sys.setHost("https://test.tacc.io/bucket");
        sys.setBucketName("testBucket");
        sys.setPort(9000);
        sys.setAuthnCredential(creds);

        S3DataClient client = new S3DataClient(oboTenant, oboUser, sys);
        URI tmpURI = client.configEndpoint(sys.getHost());
        Assert.assertEquals(tmpURI.getScheme(), "https");
        Assert.assertEquals(tmpURI.toString(), "https://test.tacc.io:9000");
    }

}
