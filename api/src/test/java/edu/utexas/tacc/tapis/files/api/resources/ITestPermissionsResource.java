package edu.utexas.tacc.tapis.files.api.resources;


import edu.utexas.tacc.tapis.files.api.BaseResourceConfig;
import edu.utexas.tacc.tapis.files.api.models.CreatePermissionRequest;
import edu.utexas.tacc.tapis.files.api.providers.FilePermissionsAuthz;
import edu.utexas.tacc.tapis.files.client.gen.model.FilePermission;
import edu.utexas.tacc.tapis.files.client.gen.model.FilePermissionResponse;
import edu.utexas.tacc.tapis.files.lib.caches.FilePermsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.models.FilePermissionsEnum;
import edu.utexas.tacc.tapis.files.lib.services.FilePermsService;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.security.ServiceJWT;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.shared.ssh.SSHConnectionCache;
import edu.utexas.tacc.tapis.sharedapi.jaxrs.filters.JWTValidateRequestFilter;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import edu.utexas.tacc.tapis.systems.client.gen.model.TransferMethodEnum;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Site;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;
import org.apache.commons.codec.Charsets;
import org.apache.commons.io.IOUtils;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTestNg;
import org.glassfish.jersey.test.TestProperties;
import org.mockito.Mockito;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.inject.Singleton;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@Test(groups = {"integration"})
public class ITestPermissionsResource extends JerseyTestNg.ContainerPerClassTest {


    private TSystem testSystem;
    private Tenant tenant;
    private Credential creds;
    private Map<String, Tenant> tenantMap = new HashMap<>();
    private Site testSite;

    // mocking out the services
    private SystemsClient systemsClient;
    private SKClient skClient;
    private TenantManager tenantManager;
    private ServiceJWT serviceJWT;
    private String user1jwt;
    private String user2jwt;


    private ITestPermissionsResource() throws Exception {
        creds = new Credential();
        creds.setAccessKey("user");
        creds.setAccessSecret("password");
        testSystem = new TSystem();
        testSystem.setHost("http://localhost");
        testSystem.setPort(9000);
        testSystem.setBucketName("test");
        testSystem.setId("testSystem");
        testSystem.setAuthnCredential(creds);
        testSystem.setRootDir("/");;
        List<TransferMethodEnum> transferMechs = new ArrayList<>();
        transferMechs.add(TransferMethodEnum.S3);
        testSystem.setTransferMethods(transferMechs);

        tenant = new Tenant();
        tenant.setTenantId("dev");
        tenant.setBaseUrl("https://test.tapis.io");
        tenant.setSiteId("test");
        tenantMap.put(tenant.getTenantId(), tenant);

        testSite = new Site();
        testSite.setSiteId("test");

    }

    @BeforeClass
    public void setUpUsers() throws Exception {
        user1jwt = IOUtils.resourceToString("/user1jwt", Charsets.UTF_8);
        user2jwt = IOUtils.resourceToString("/user2jwt", Charsets.UTF_8);
    }

    @Override
    protected ResourceConfig configure() {
        enable(TestProperties.LOG_TRAFFIC);
        enable(TestProperties.DUMP_ENTITY);
        tenantManager = Mockito.mock(TenantManager.class);
        skClient = Mockito.mock(SKClient.class);
        systemsClient = Mockito.mock(SystemsClient.class);
        serviceJWT = Mockito.mock(ServiceJWT.class);
        JWTValidateRequestFilter.setSiteId("test");
        JWTValidateRequestFilter.setService("files");
        ResourceConfig app = new BaseResourceConfig()
            .register(new JWTValidateRequestFilter(tenantManager))
            .register(FilePermissionsAuthz.class)
            .register(new AbstractBinder() {
                @Override
                protected void configure() {
                    bind(systemsClient).to(SystemsClient.class);
                    bind(skClient).to(SKClient.class);
                    bind(tenantManager).to(TenantManager.class);
                    bind(serviceJWT).to(ServiceJWT.class);
                    bindAsContract(FilePermsService.class).in(Singleton.class);
                    bindAsContract(FilePermsCache.class).in(Singleton.class);
                    bindAsContract(SystemsCache.class).in(Singleton.class);
                }
            });

        app.register(PermissionsApiResource.class);
        return app;
    }


    @Test
    public void testAuthzAnnotation() throws Exception {
        /*
        Test the authz filter for permissions operations. The system is owned by testuser2,
        but testuser1 is making the request. Should return 403
         */
        testSystem.setOwner("testuser2");
        when(tenantManager.getTenants()).thenReturn(tenantMap);
        when(tenantManager.getTenant(any())).thenReturn(tenant);
        when(tenantManager.getSite(any())).thenReturn(testSite);
        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
        CreatePermissionRequest req = new CreatePermissionRequest();
        req.setPermission(FilePermissionsEnum.ALL);
        req.setUsername("testuser3");
        Response response = target("/v3/files/permissions/testSystem/a/")
            .queryParam("username", "testuser3")
            .request()
            .header("X-Tapis-Token", user1jwt)
            .post(Entity.json(req));
        Assert.assertEquals(response.getStatus(), 403);
    }

    @Test
    public void testAuthzAnnotationSuccess() throws Exception {
        /*
        Test the authz filter for permissions operations. Should return 200
         */
        testSystem.setOwner("testuser1");
        when(tenantManager.getTenants()).thenReturn(tenantMap);
        when(tenantManager.getTenant(any())).thenReturn(tenant);
        when(tenantManager.getSite(any())).thenReturn(testSite);
        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
        CreatePermissionRequest req = new CreatePermissionRequest();
        req.setPermission(FilePermissionsEnum.ALL);
        req.setUsername("testuser3");
        Response response = target("/v3/files/permissions/testSystem/a/")
            .queryParam("username", "testuser3")
            .request()
            .header("X-Tapis-Token", user1jwt)
            .post(Entity.json(req));
        Assert.assertEquals(response.getStatus(), 200);
    }

    @Test
    public void testAddPermissions() throws Exception {
        testSystem.setOwner("testuser1");
        when(tenantManager.getTenants()).thenReturn(tenantMap);
        when(tenantManager.getTenant(any())).thenReturn(tenant);
        when(tenantManager.getSite(any())).thenReturn(testSite);
        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
        CreatePermissionRequest req = new CreatePermissionRequest();
        req.setPermission(FilePermissionsEnum.ALL);
        req.setUsername("testuser3");
        Response response = target("/v3/files/permissions/testSystem/a/")
            .queryParam("username", "testuser3")
            .request()
            .header("X-Tapis-Token", user1jwt)
            .post(Entity.json(req));
        Assert.assertEquals(response.getStatus(), 200);
        Mockito.verify(skClient).grantUserPermission("dev", "testuser3", "files:dev:ALL:testSystem:a");
    }

    @Test
    public void testGetUserPermissionsAsOwner() throws Exception {

        // Owners of the system can check permissions for other users. Normal API users only can only
        // see get their own permissions. testuser1, the owner of the system is making this request, but checking
        // to see what permissions testuser3 has.
        testSystem.setOwner("testuser1");
        when(tenantManager.getTenants()).thenReturn(tenantMap);
        when(tenantManager.getTenant(any())).thenReturn(tenant);
        when(tenantManager.getSite(any())).thenReturn(testSite);
        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
        Response response = target("/v3/files/permissions/testSystem/a/")
            .queryParam("username", "testuser3")
            .request()
            .header("X-Tapis-Token", user1jwt)
            .get();
        Assert.assertEquals(response.getStatus(), 200);
        Mockito.verify(skClient).isPermitted("dev", "testuser3", "files:dev:*:testSystem:a");
    }

    @Test
    public void testGetUserPermissionsAsUser() throws Exception {

        // Owners of the system can check permissions for other users. Normal API users only can only
        // see get their own permissions. testuser2, the owner of the system is making this request, but checking
        // to see what permissions the API user, testuser1 has.
        testSystem.setOwner("testuser2");
        when(tenantManager.getTenants()).thenReturn(tenantMap);
        when(tenantManager.getTenant(any())).thenReturn(tenant);
        when(tenantManager.getSite(any())).thenReturn(testSite);
        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);

        when(skClient.isPermitted(any(), any(), any())).thenReturn(true);
        FilePermissionResponse response = target("/v3/files/permissions/testSystem/a/")
            .queryParam("username", "testuser3")
            .request()
            .header("X-Tapis-Token", user1jwt)
            .get(FilePermissionResponse.class);
        Assert.assertEquals(response.getStatus(), "success");

        Mockito.verify(skClient).isPermitted("dev", "testuser1", "files:dev:*:testSystem:a");
        Assert.assertEquals(response.getResult().getPermissions(), FilePermission.PermissionsEnum.ALL);
    }

    @Test
    public void testDeletePermsNoUsername() throws Exception {

        testSystem.setOwner("testuser1");
        when(tenantManager.getTenants()).thenReturn(tenantMap);
        when(tenantManager.getTenant(any())).thenReturn(tenant);
        when(tenantManager.getSite(any())).thenReturn(testSite);
        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);

        Response response = target("/v3/files/permissions/testSystem/a/")
            .request()
            .header("X-Tapis-Token", user1jwt)
            .delete();
        Assert.assertEquals(response.getStatus(), 400);

    }
}
