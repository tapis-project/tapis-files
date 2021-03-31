package edu.utexas.tacc.tapis.files.api.resources;


import edu.utexas.tacc.tapis.files.api.BaseResourceConfig;
import edu.utexas.tacc.tapis.files.api.models.CreatePermissionRequest;
import edu.utexas.tacc.tapis.files.api.providers.FilePermissionsAuthz;
import edu.utexas.tacc.tapis.files.lib.caches.FilePermsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo.Permission;
import edu.utexas.tacc.tapis.files.lib.models.FilePermission;
import edu.utexas.tacc.tapis.files.lib.services.FilePermsService;
import edu.utexas.tacc.tapis.files.lib.utils.TenantCacheFactory;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.security.ServiceJWT;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.sharedapi.jaxrs.filters.JWTValidateRequestFilter;
import edu.utexas.tacc.tapis.sharedapi.responses.TapisResponse;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import edu.utexas.tacc.tapis.systems.client.gen.model.TransferMethodEnum;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Site;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.mockito.Mockito;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

@Test(groups={"integration"})
public class ITestPermissionsResource extends BaseDatabaseIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(ITestPermissionsResource.class);
    private TSystem testSystem;
    private Tenant tenant;
    private Credential creds;
    private Map<String, Tenant> tenantMap = new HashMap<>();
    private Site testSite;

    // mocking out the services
    private SystemsClient systemsClient;
    private SKClient skClient;
    private ServiceJWT serviceJWT;
    private static class FilePermissionResponse extends TapisResponse<FilePermission> {}


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
        testSystem.setRootDir("/");
        List<TransferMethodEnum> transferMechs = new ArrayList<>();
        transferMechs.add(TransferMethodEnum.S3);
        testSystem.setTransferMethods(transferMechs);

    }

    @BeforeClass
    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected ResourceConfig configure() {
        forceSet(TestProperties.CONTAINER_PORT, "0");
        enable(TestProperties.LOG_TRAFFIC);
        enable(TestProperties.DUMP_ENTITY);
        skClient = Mockito.mock(SKClient.class);
        systemsClient = Mockito.mock(SystemsClient.class);
        serviceJWT = Mockito.mock(ServiceJWT.class);
        JWTValidateRequestFilter.setSiteId("tacc");
        JWTValidateRequestFilter.setService("files");
        ResourceConfig app = new BaseResourceConfig()
            .register(JWTValidateRequestFilter.class)
            .register(FilePermissionsAuthz.class)
            .register(new AbstractBinder() {
                @Override
                protected void configure() {
                    bind(systemsClient).to(SystemsClient.class);
                    bind(skClient).to(SKClient.class);
                    bindFactory(TenantCacheFactory.class).to(TenantManager.class).in(Singleton.class);
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
    public void testPermissionsWithServiceJwt() throws Exception {
        testSystem.setOwner("testuser2");
        log.info(getServiceJwt());
        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
        when(skClient.isPermitted(any(), any(), any())).thenReturn(true);
        try {
            FilePermissionResponse response = target("/v3/files/permissions/testSystem/a/")
                .queryParam("username", "testuser2")
                .request()
                .header("X-Tapis-Token", getServiceJwt())
                .header("x-tapis-tenant", "dev")
                .header("x-tapis-user", "testuser2")
                .get(FilePermissionResponse.class);
            Assert.assertEquals(response.getResult().getPermission(), Permission.MODIFY);
        } catch (Exception e) {
            log.info(e.getMessage(), e);
        }
    }


    /**
     * This test uses a serviceJWT to make a request to the permissions GET route
     * OBO testuser2, but for a system that is mocked to be owned by testuser1
     * so this should throw a 403 since testuser2 has no access to the system.
     * @throws Exception
     */
    @Test
    public void testPermissionsWithServiceJwtShould403() throws Exception {
        testSystem.setOwner("testuser1");
        when(systemsClient.getUserCredential(any(), any())).thenReturn(creds);
        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
        when(skClient.isPermitted(any(), any(), any())).thenReturn(true);
        FilePermissionResponse response = target("/v3/files/permissions/testSystem/a/")
            .queryParam("username", "testuser2")
            .request()
            .header("X-Tapis-Token", getServiceJwt())
            .header("x-tapis-tenant", "dev")
            .header("x-tapis-user", "testuser2")
            .get(FilePermissionResponse.class);
        Assert.assertEquals(response.getResult().getPermission(), Permission.MODIFY);
    }

    @Test
    public void testAuthzAnnotation() throws Exception {
        /*
        Test the authz filter for permissions operations. The system is owned by testuser2,
        but testuser1 is making the request. Should return 403
         */
        testSystem.setOwner("testuser2");
        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
        when(systemsClient.getUserCredential(any(), any())).thenReturn(creds);

        CreatePermissionRequest req = new CreatePermissionRequest();
        req.setPermission(Permission.MODIFY);
        req.setUsername("testuser3");
        Response response = target("/v3/files/permissions/testSystem/a/")
            .queryParam("username", "testuser3")
            .request()
            .header("X-Tapis-Token", getJwtForUser("dev", "testuser1"))
            .post(Entity.json(req));
        Assert.assertEquals(response.getStatus(), 403);
    }

    @Test
    public void testAuthzAnnotationSuccess() throws Exception {
        /*
        Test the authz filter for permissions operations. Should return 200
         */
        testSystem.setOwner("testuser1");
        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
        CreatePermissionRequest req = new CreatePermissionRequest();
        req.setPermission(Permission.MODIFY);
        req.setUsername("testuser3");
        Response response = target("/v3/files/permissions/testSystem/a/")
            .queryParam("username", "testuser3")
            .request()
            .header("X-Tapis-Token", getJwtForUser("dev", "testuser1"))
            .post(Entity.json(req));
        Assert.assertEquals(response.getStatus(), 200);
    }

    @Test
    public void testAddPermissions() throws Exception {
        testSystem.setOwner("testuser1");
        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
        CreatePermissionRequest req = new CreatePermissionRequest();
        req.setPermission(Permission.MODIFY);
        req.setUsername("testuser3");
        Response response = target("/v3/files/permissions/testSystem/a/")
            .queryParam("username", "testuser3")
            .request()
            .header("X-Tapis-Token", getJwtForUser("dev", "testuser1"))
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
        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);
        Response response = target("/v3/files/permissions/testSystem/a/")
            .queryParam("username", "testuser3")
            .request()
            .header("X-Tapis-Token", getJwtForUser("dev", "testuser1"))
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
        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);

        when(skClient.isPermitted(any(), any(), any())).thenReturn(true);
        FilePermissionResponse response = target("/v3/files/permissions/testSystem/a/")
            .queryParam("username", "testuser3")
            .request()
            .header("X-Tapis-Token", getJwtForUser("dev", "testuser1"))
            .get(FilePermissionResponse.class);
        Assert.assertEquals(response.getStatus(), "success");

        //Verify that the SK got called with the correct things
        Mockito.verify(skClient).isPermitted("dev", "testuser3", "files:dev:*:testSystem:a");
        Assert.assertEquals(response.getResult().getPermission(), Permission.MODIFY);
        Assert.assertEquals(response.getResult().getUsername(), "testuser3");
    }

    @Test
    public void testGetUserPermissionsAsDifferentUser() throws Exception {

        // Owners of the system can check permissions for other users. Normal API users only can only
        // see get their own permissions.

        //testuser1 is a user of the system,
        // testuser2 is the owner
        // testuser3 is who testuser1 is asking about
        testSystem.setOwner("testuser2");
        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);

        when(skClient.isPermitted(any(), any(), any())).thenReturn(true);
        FilePermissionResponse response = target("/v3/files/permissions/testSystem/a/")
            .queryParam("username", "testuser3")
            .request()
            .header("X-Tapis-Token", getJwtForUser("dev", "testuser1"))
            .get(FilePermissionResponse.class);

        //Verify that the SK got called with the correct things
        Mockito.verify(skClient).isPermitted("dev", "testuser3", "files:dev:*:testSystem:a");
        Assert.assertEquals(response.getResult().getPermission(), Permission.MODIFY);
        Assert.assertEquals(response.getResult().getUsername(), "testuser3");
    }

    @Test
    public void testDeletePermsNoUsername() throws Exception {

        testSystem.setOwner("testuser1");
        when(systemsClient.getSystemWithCredentials(any(), any())).thenReturn(testSystem);

        Response response = target("/v3/files/permissions/testSystem/a/")
            .request()
            .header("X-Tapis-Token", getJwtForUser("dev", "testuser1"))
            .delete();
        Assert.assertEquals(response.getStatus(), 400);

    }
}
