package edu.utexas.tacc.tapis.files.lib.clients;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.InputStream;

@Test(groups={"integration"})
public class HTTPClientTests
{
  @Test
  public void testGetFile() throws Exception
  {
    String sourceUri = "https://google.com";
    String destUri = sourceUri;
    HTTPClient client = new HTTPClient("testTenant", "testUser", sourceUri, destUri);
    InputStream stream = client.getStream(sourceUri);
    Assert.assertNotNull(stream);
  }
}
