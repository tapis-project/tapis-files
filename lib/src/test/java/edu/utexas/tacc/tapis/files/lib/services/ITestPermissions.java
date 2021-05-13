package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups={"e2e"})
public class ITestPermissions {

    public void testSKGrantOnFile() throws Exception {
        IRuntimeConfig runtimeConfig = RuntimeSettings.get();
        TenantManager tenantManager = TenantManager.getInstance(runtimeConfig.getTenantsServiceURL());
        tenantManager.getTenants();
        ServiceContext context = ServiceContext.getInstance();
        context.initServiceJWT(runtimeConfig.getSiteId(), "files", runtimeConfig.getServicePassword());

        ServiceClients serviceClients = ServiceClients.getInstance();
        SKClient skClient = serviceClients.getClient("testuser2", "dev", SKClient.class);
        int recs = skClient.grantUserPermission("dev", "testuser1", "files:dev:READ:testSystem:/dir1/dir2/testFile.txt");
        Assert.assertTrue(recs > 0);
        boolean isPermitted = skClient.isPermitted("dev", "testuser1", "files:dev:READ:testSystem:/dir1/dir2/testFile.txt");
        Assert.assertTrue(isPermitted);
    }

    public void testSKGrantOnSubDir() throws Exception {
        IRuntimeConfig runtimeConfig = RuntimeSettings.get();
        TenantManager tenantManager = TenantManager.getInstance(runtimeConfig.getTenantsServiceURL());
        tenantManager.getTenants();
        ServiceContext context = ServiceContext.getInstance();
        context.initServiceJWT(runtimeConfig.getSiteId(), "files", runtimeConfig.getServicePassword());

        ServiceClients serviceClients = ServiceClients.getInstance();
        SKClient skClient = serviceClients.getClient("testuser2", "dev", SKClient.class);
        int recs = skClient.grantUserPermission("dev", "testuser1", "files:dev:READ:testSystem2:/dir1/dir2/");
        boolean isPermitted = skClient.isPermitted("dev", "testuser1", "files:dev:READ:testSystem2:/dir1/dir2/testFile.txt");
        Assert.assertTrue(isPermitted);
    }




}
