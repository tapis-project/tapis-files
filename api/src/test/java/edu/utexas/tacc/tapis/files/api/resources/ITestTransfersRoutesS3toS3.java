package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.files.api.BaseResourceConfig;
import edu.utexas.tacc.tapis.files.api.models.TransferTaskRequest;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskRequestElement;
import edu.utexas.tacc.tapis.files.lib.caches.FilePermsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.services.FilePermsService;
import edu.utexas.tacc.tapis.shared.ssh.SSHConnectionCache;
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
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Site;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;
import edu.utexas.tacc.tapis.systems.client.gen.model.TransferMethodEnum;
import org.apache.commons.codec.Charsets;
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
import org.apache.commons.io.IOUtils;


import javax.inject.Singleton;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@Test(groups={"integration"})
public class ITestTransfersRoutesS3toS3 extends BaseDatabaseIntegrationTest {

    private Logger log = LoggerFactory.getLogger(ITestTransfersRoutesS3toS3.class);
    private String user1jwt;
    private String user2jwt;
    private TSystem testSystem;
    private Credential creds;
    private static class TransferTaskResponse extends TapisResponse<TransferTask>{}
    // mocking out the services
    private SystemsClient systemsClient;
    private SKClient skClient;
    private TenantManager tenantManager;
    private ServiceJWT serviceJWT;
    private Tenant tenant;
    private Site testSite;
    private Map<String, Tenant> tenantMap = new HashMap<>();


    private ITestTransfersRoutesS3toS3() throws Exception {
        //List<String> creds = new ArrayList<>();
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

        tenant = new Tenant();
        tenant.setTenantId("testTenant");
        tenant.setBaseUrl("test.tapis.io");
        tenantMap.put(tenant.getTenantId(), tenant);

        testSite = new Site();
        testSite.setSiteId("test");
    }

    @Override
    protected ResourceConfig configure() {
        enable(TestProperties.LOG_TRAFFIC);
        enable(TestProperties.DUMP_ENTITY);
        forceSet(TestProperties.CONTAINER_PORT, "0");
        tenantManager = Mockito.mock(TenantManager.class);
        skClient = Mockito.mock(SKClient.class);
        systemsClient = Mockito.mock(SystemsClient.class);
        serviceJWT = Mockito.mock(ServiceJWT.class);
        JWTValidateRequestFilter.setSiteId("dev");
        JWTValidateRequestFilter.setService("files");
        ResourceConfig app = new BaseResourceConfig()
                .register(new JWTValidateRequestFilter(tenantManager))
                .register(new AbstractBinder() {
                    @Override
                    protected void configure() {
                        bind(systemsClient).to(SystemsClient.class);
                        bind(skClient).to(SKClient.class);
                        bind(serviceJWT).to(ServiceJWT.class);
                        bind(tenantManager).to(TenantManager.class);
                        bindAsContract(SystemsCache.class).in(Singleton.class);
                        bindAsContract(FilePermsCache.class).in(Singleton.class);
                        bindAsContract(FilePermsService.class).in(Singleton.class);
                        bindAsContract(TransfersService.class).in(Singleton.class);
                        bindAsContract(FileTransfersDAO.class);
                        bindAsContract(RemoteDataClientFactory.class);
                        bind(new SSHConnectionCache(1, TimeUnit.MINUTES)).to(SSHConnectionCache.class);

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


    @BeforeClass
    public void setUpUsers() throws Exception {
        user1jwt = IOUtils.resourceToString("/user1jwt", Charsets.UTF_8);
        user2jwt = IOUtils.resourceToString("/user2jwt", Charsets.UTF_8);
    }

    @BeforeMethod
    public void initMocks() throws Exception {
        when(tenantManager.getTenants()).thenReturn(tenantMap);
        when(tenantManager.getTenant(any())).thenReturn(tenant);
        when(systemsClient.getUserCredential(any(), any())).thenReturn(creds);
        when(skClient.isPermitted(any(), any(String.class), any(String.class))).thenReturn(true);
        when(systemsClient.getSystemWithCredentials(any(String.class), any())).thenReturn(testSystem);
        when(tenantManager.getSite(any())).thenReturn(testSite);

    }



    /**
     * Helper method to create transfer tasks
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
                .header("x-tapis-token", user1jwt)
                .post(Entity.json(request), TransferTaskResponse.class);
        return createTaskResponse.getResult();
    }

    private TransferTask getTransferTask(String taskUUID) {
        TransferTaskResponse getTaskResponse = target("/v3/files/transfers/" +taskUUID)
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .header("x-tapis-token", user1jwt)
                .get(TransferTaskResponse.class);

        TransferTask task = getTaskResponse.getResult();
        return task;

    }

    @Test
    public void postTransferTask() throws Exception{
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
                .header("x-tapis-token", user1jwt)
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
                .header("x-tapis-token", user1jwt)
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
                .header("x-tapis-token", user1jwt)
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
                .header("x-tapis-token", user1jwt)
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
                .header("x-tapis-token", user1jwt)
                .delete();

        Assert.assertEquals(resp.getStatus(), 404);

    }


}
