package edu.utexas.tacc.tapis.files.lib.cache;


import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.security.ServiceJWT;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;


@Test(enabled = false)
public class TestSystemsCache {

    private final SystemsClient systemsClient = Mockito.mock(SystemsClient.class);
    private final ServiceClients serviceClients = Mockito.mock(ServiceClients.class);

    @BeforeMethod
    public void beforeTestMethod() throws Exception {

        when(serviceClients.getClient(any(), any(), eq(SystemsClient.class))).thenReturn(systemsClient);

        TapisSystem testSystem = new TapisSystem();
        testSystem.setId("12345");
        testSystem.setHost("test.edu");
        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);

    }

    @Test()
    public void testCacheLoader() throws Exception {
        SystemsCache cache = new SystemsCache(serviceClients);
        TapisSystem check = cache.getSystem("testTenant", "testSystem", "testUser");
        Assert.assertEquals(check.getId(), "12345");

    }

}
