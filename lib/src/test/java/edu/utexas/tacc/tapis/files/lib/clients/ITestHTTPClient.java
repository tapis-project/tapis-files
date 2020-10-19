package edu.utexas.tacc.tapis.files.lib.clients;

import org.testng.annotations.Test;

@Test(groups={"integration"})
public class ITestHTTPClient {

    @Test
    public void testGetFile() throws Exception {
        HTTPClient client = new HTTPClient();
        client.getStream("google.com");

    }


}
