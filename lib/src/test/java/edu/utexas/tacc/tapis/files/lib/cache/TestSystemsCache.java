package edu.utexas.tacc.tapis.files.lib.cache;


import edu.utexas.tacc.tapis.shared.security.ServiceJWT;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import org.mockito.Mockito;
import org.testng.annotations.Test;

@Test
public class TestSystemsCache {

    private final ServiceJWT serviceJWT = Mockito.mock(ServiceJWT.class);
    private final SystemsClient systemsClient = Mockito.mock(SystemsClient.class);

    @Test
    public void testCacheLoader() throws Exception {
        


    }

}
