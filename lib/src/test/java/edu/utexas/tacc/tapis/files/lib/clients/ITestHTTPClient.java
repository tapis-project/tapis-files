package edu.utexas.tacc.tapis.files.lib.clients;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.InputStream;

@Test(groups={"integration"})
public class ITestHTTPClient {

    @Test
    public void testGetFile() throws Exception {
        HTTPClient client = new HTTPClient();
        InputStream stream = client.getStream("https://google.com");
        Assert.assertNotNull(stream);
    }
}
