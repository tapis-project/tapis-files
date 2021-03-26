package edu.utexas.tacc.tapis.files.lib.clients;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.InputStream;

@Test(groups={"integration"})
public class ITestHTTPClient {

    @Test
    public void testGetFile() throws Exception {
        String sourceUri = "https://google.com";
        HTTPClient client = new HTTPClient("testTenant", "testUser", sourceUri);
        InputStream stream = client.getStream(sourceUri);
        Assert.assertNotNull(stream);
    }
}
