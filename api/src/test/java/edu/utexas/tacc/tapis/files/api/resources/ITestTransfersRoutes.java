package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.files.api.BaseResourceConfig;
import edu.utexas.tacc.tapis.files.api.models.TransferTaskRequest;
import edu.utexas.tacc.tapis.files.lib.caches.SSHConnectionCache;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskRequestElement;
import edu.utexas.tacc.tapis.files.lib.caches.FilePermsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.services.FilePermsService;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.sharedapi.jaxrs.filters.JWTValidateRequestFilter;
import edu.utexas.tacc.tapis.sharedapi.responses.TapisResponse;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import edu.utexas.tacc.tapis.files.lib.services.TransfersService;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.security.ServiceJWT;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


import javax.inject.Singleton;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@Test(groups = {"integration"})
public class ITestTransfersRoutes extends BaseDatabaseIntegrationTest {

    private final Logger log = LoggerFactory.getLogger(ITestTransfersRoutes.class);
    private final TapisSystem testSystem;
    private final Credential creds;

    private static class TransferTaskResponse extends TapisResponse<TransferTask> {
    }

    // mocking out the services
    private ServiceClients serviceClients;
    private SystemsClient systemsClient;
    private SKClient skClient;
    private TenantManager tenantManager;
    private ServiceJWT serviceJWT;
    private final Tenant tenant;
    private final Site testSite;
    private SystemsCache systemsCache;
    private final Map<String, Tenant> tenantMap = new HashMap<>();


    private ITestTransfersRoutes() throws Exception {
        //List<String> creds = new ArrayList<>();
        creds = new Credential();
        creds.setAccessKey("user");
        creds.setAccessSecret("password");
        testSystem = new TapisSystem();
        testSystem.setHost("http://localhost");
        testSystem.setPort(9000);
        testSystem.setBucketName("test");
        testSystem.setId("testSystem");
        testSystem.setAuthnCredential(creds);
        testSystem.setRootDir("/");

        tenant = new Tenant();
        tenant.setTenantId("testTenant");
        tenant.setBaseUrl("test.tapis.io");
        tenantMap.put(tenant.getTenantId(), tenant);

        testSite = new Site();
        testSite.setSiteId("tacc");
    }

    @Override
    protected ResourceConfig configure() {
        enable(TestProperties.LOG_TRAFFIC);
        enable(TestProperties.DUMP_ENTITY);
        forceSet(TestProperties.CONTAINER_PORT, "0");
        tenantManager = Mockito.mock(TenantManager.class);
        skClient = Mockito.mock(SKClient.class);
        serviceClients = Mockito.mock(ServiceClients.class);
        systemsClient = Mockito.mock(SystemsClient.class);
        serviceJWT = Mockito.mock(ServiceJWT.class);
        systemsCache = Mockito.mock(SystemsCache.class);
        JWTValidateRequestFilter.setSiteId("tacc");
        JWTValidateRequestFilter.setService("files");
        ResourceConfig app = new BaseResourceConfig()
            .register(new JWTValidateRequestFilter(tenantManager))
            .register(new AbstractBinder() {
                @Override
                protected void configure() {
                    bind(new SSHConnectionCache(1, TimeUnit.MINUTES)).to(SSHConnectionCache.class);
                    bind(serviceClients).to(ServiceClients.class);
                    bind(systemsClient).to(SystemsClient.class);
                    bind(serviceJWT).to(ServiceJWT.class);
                    bind(tenantManager).to(TenantManager.class);
                    bind(systemsCache).to(SystemsCache.class);
                    bindAsContract(FilePermsCache.class).in(Singleton.class);
                    bindAsContract(FilePermsService.class).in(Singleton.class);
                    bindAsContract(TransfersService.class).in(Singleton.class);
                    bindAsContract(FileTransfersDAO.class);
                    bindAsContract(RemoteDataClientFactory.class);

                }
            });
        app.register(TransfersApiResource.class);
        return app;
    }

    @BeforeClass
    @Override
    public void setUp() throws Exception {
        super.setUp();
    }


    @BeforeMethod
    public void initMocks() throws Exception {
        when(tenantManager.getTenants()).thenReturn(tenantMap);
        when(tenantManager.getTenant(any())).thenReturn(tenant);
        when(serviceClients.getClient(any(String.class), any(String.class), eq(SKClient.class))).thenReturn(skClient);
        when(systemsClient.getUserCredential(any(), any())).thenReturn(creds);
        when(skClient.isPermitted(any(), any(String.class), any(String.class))).thenReturn(true);
        when(systemsClient.getSystemWithCredentials(any(String.class), any())).thenReturn(testSystem);
        when(tenantManager.getSite(any())).thenReturn(testSite);
        when(systemsCache.getSystem(any(), any(), any())).thenReturn(testSystem);
    }


    /**
     * Helper method to create transfer tasks
     *
     * @return
     */
    private TransferTask createTransferTask() {
        TransferTaskRequest request = new TransferTaskRequest();
        request.setTag("testTag");
        TransferTaskRequestElement element1 = new TransferTaskRequestElement();
        element1.setSourceURI("tapis://test.edu/sourceSystem/sourcePath");
        element1.setDestinationURI("tapis://tests.edu/destSystem/destinationPath");
        List<TransferTaskRequestElement> elements = new ArrayList<>();
        elements.add(element1);
        request.setElements(elements);


        TransferTaskResponse createTaskResponse = target("/v3/files/transfers")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("content-type", MediaType.APPLICATION_JSON)
            .header("X-Tapis-Token", getJwtForUser("dev", "testuser1"))
            .post(Entity.json(request), TransferTaskResponse.class);
        return createTaskResponse.getResult();
    }

    private TransferTask getTransferTask(String taskUUID) {
        TransferTaskResponse getTaskResponse = target("/v3/files/transfers/" + taskUUID)
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("X-Tapis-Token", getJwtForUser("dev", "testuser1"))
            .get(TransferTaskResponse.class);

        TransferTask task = getTaskResponse.getResult();
        return task;

    }

    @Test
    public void postTransferTask() throws Exception {
        TransferTask newTask = createTransferTask();

        Assert.assertNotNull(newTask.getUuid());
        Assert.assertNotNull(newTask.getCreated());
        Assert.assertEquals(newTask.getParentTasks().size(), 1);
        Assert.assertEquals(newTask.getUsername(), "testuser1");
        Assert.assertEquals(newTask.getTenantId(), "dev");
        Assert.assertEquals(newTask.getStatus(), TransferTaskStatus.ACCEPTED);
    }

    @Test
    public void getTransferById() throws Exception {

        TransferTask t = createTransferTask();

        TransferTask task = getTransferTask(t.getUuid().toString());
        Assert.assertNotNull(task.getUuid());
        Assert.assertNotNull(task.getCreated());
    }

    @Test
    public void testGetTransferDetails() throws Exception {

        TransferTask t = createTransferTask();

        Response response = target("/v3/files/transfers/" + t.getUuid().toString() + "/details")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("X-Tapis-Token", getJwtForUser("dev", "testuser1"))
            .get();

        TransferTaskResponse data = response.readEntity(TransferTaskResponse.class);
        TransferTask task = data.getResult();
        Assert.assertNotNull(task.getParentTasks());
        Assert.assertEquals(task.getParentTasks().size(), 1);
    }


    /**
     * This request should throw a 400 as the ValidUUID validator will fail
     */
    @Test
    public void getTransfer400() {

        Response response = target("/v3/files/transfers/INVALID")
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("X-Tapis-Token", getJwtForUser("dev", "testuser1"))
            .get();

        TransferTaskResponse data = response.readEntity(TransferTaskResponse.class);
        Assert.assertEquals(response.getStatus(), 400);
        Assert.assertEquals(data.getStatus(), "error");
    }

    /**
     * Valid UUID but not found should be 404
     */
    @Test
    public void getTransfer404() {
        String uuid = UUID.randomUUID().toString();
        Response response = target("/v3/files/transfers/" + uuid)
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("X-Tapis-Token", getJwtForUser("dev", "testuser1"))
            .get();

        TransferTaskResponse data = response.readEntity(TransferTaskResponse.class);
        Assert.assertEquals(response.getStatus(), 404);
        Assert.assertEquals(data.getStatus(), "error");
    }

    @Test
    public void deleteTransfer() throws Exception {
        TransferTask t = createTransferTask();
        Response resp = target("/v3/files/transfers/" + t.getUuid().toString())
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("X-Tapis-Token", getJwtForUser("dev", "testuser1"))
            .delete();

        TransferTask task = getTransferTask(t.getUuid().toString());
        Assert.assertEquals(task.getStatus(), TransferTaskStatus.CANCELLED);
        Assert.assertEquals(resp.getStatus(), 200);

    }

    @Test
    public void deleteTransfer404() throws Exception {
        Response resp = target("/v3/files/transfers/" + UUID.randomUUID())
            .request()
            .accept(MediaType.APPLICATION_JSON)
            .header("X-Tapis-Token", getJwtForUser("dev", "testuser1"))
            .delete();

        Assert.assertEquals(resp.getStatus(), 404);

    }


}
