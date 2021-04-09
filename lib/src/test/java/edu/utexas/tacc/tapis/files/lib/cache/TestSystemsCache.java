package edu.utexas.tacc.tapis.files.lib.cache;


import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.shared.security.ServiceJWT;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;

@Test
public class TestSystemsCache {

    private final ServiceJWT serviceJWT = Mockito.mock(ServiceJWT.class);
    private final SystemsClient systemsClient = Mockito.mock(SystemsClient.class);
    private final TenantManager tenantManager = Mockito.mock(TenantManager.class);

    @BeforeMethod
    public void beforeTestMethod() throws Exception {
        String testJWT = "1234565asd";
        when(serviceJWT.getAccessJWT(any())).thenReturn(testJWT);

        TapisSystem testSystem = new TapisSystem();
        testSystem.setId("12345");
        testSystem.setHost("test.edu");
        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);

        Tenant testTenant = new Tenant();
        testTenant.setBaseUrl("test.edu");
        when(tenantManager.getTenant(any())).thenReturn(testTenant);
    }

    @Test
    public void testCacheLoader() throws Exception {
        SystemsCache cache = new SystemsCache(systemsClient, serviceJWT, tenantManager);
        TapisSystem check = cache.getSystem("testTenant", "testSystem", "testUser");
        Assert.assertEquals(check.getId(), "12345");

    }

}
