package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.files.api.BaseResourceConfig;
import edu.utexas.tacc.tapis.files.api.models.PostItCreateRequest;
import edu.utexas.tacc.tapis.files.api.models.PostItUpdateRequest;
import edu.utexas.tacc.tapis.files.api.providers.FilePermissionsAuthz;
import edu.utexas.tacc.tapis.files.lib.caches.SSHConnectionCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.caches.TenantAdminCache;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.dao.postits.PostItsDAO;
import edu.utexas.tacc.tapis.files.lib.models.PostIt;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService;
import edu.utexas.tacc.tapis.files.lib.services.FilePermsService;
import edu.utexas.tacc.tapis.files.lib.services.FileShareService;
import edu.utexas.tacc.tapis.files.lib.services.PostItsService;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.sharedapi.jaxrs.filters.JWTValidateRequestFilter;
import edu.utexas.tacc.tapis.sharedapi.responses.TapisResponse;
import edu.utexas.tacc.tapis.systems.client.gen.model.AuthnEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.HttpUrlConnectorProvider;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.Assert;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.inject.Singleton;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Test(groups = {"integration"})
public class TestPostItsResource extends BaseDatabaseIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(TestPostItsResource.class);
    private static final String TENANT = "dev";
    private static final String POSTITS_ROUTE = "v3/files/postits";
    private static final String OPS_ROUTE = "/v3/files/ops";
    private static final String SYSTEM_ID = "testSystem";
    private static final String TEST_USR1 = "testuser1";
    private static final String TEST_USR2 = "testuser2";
    private static final String TEST_ADMIN_USR = "testadmin";
    private final List<TapisSystem> testSystems = new ArrayList<>();
    private ServiceClients serviceClients;
    private SKClient skClient;
    private SystemsCache systemsCache;
    private FilePermsService permsService;
    private static class CreateFileResponse extends TapisResponse<String> {}
    private static class TapisPostItResponse extends TapisResponse<PostIt> {}
    private static class TapisPostItListResponse extends TapisResponse<List<PostIt>> {}

    TestPostItsResource() {
        // Create Tapis Systems used in test.
        Credential creds;
        //SSH system with username+password
        creds = new Credential();
        creds.setAccessKey("testuser");
        creds.setPassword("password");

        TapisSystem testSystemSSH1 = new TapisSystem();
        testSystemSSH1.setId(SYSTEM_ID);
        testSystemSSH1.setSystemType(SystemTypeEnum.LINUX);
        testSystemSSH1.setOwner("testuser1");
        testSystemSSH1.setHost("localhost");
        testSystemSSH1.setPort(2222);
        testSystemSSH1.setRootDir("/data/home/testuser/");
        testSystemSSH1.setEffectiveUserId("testuser");
        testSystemSSH1.setDefaultAuthnMethod(AuthnEnum.PASSWORD);
        testSystemSSH1.setAuthnCredential(creds);

        testSystems.add(testSystemSSH1);
    }
    @Override
    protected Application configure() {
        enable(TestProperties.LOG_TRAFFIC);
        enable(TestProperties.DUMP_ENTITY);
        forceSet(TestProperties.CONTAINER_PORT, "0");
        skClient = Mockito.mock(SKClient.class);
        serviceClients = Mockito.mock(ServiceClients.class);
        systemsCache = Mockito.mock(SystemsCache.class);
        permsService = Mockito.mock(FilePermsService.class);
        JWTValidateRequestFilter.setSiteId("tacc");
        JWTValidateRequestFilter.setService("files");
        ServiceContext serviceContext = Mockito.mock(ServiceContext.class);
        IRuntimeConfig runtimeConfig = RuntimeSettings.get();
        TenantManager tenantManager = TenantManager.getInstance(runtimeConfig.getTenantsServiceURL());
        tenantManager.getTenants();

        ResourceConfig rc = new BaseResourceConfig().
                register(new AbstractBinder() {
                    @Override
                    protected void configure() {
                        bindAsContract(PostItsService.class).in(Singleton.class);
                        bindAsContract(PostItsDAO.class).in(Singleton.class);
                        bindAsContract(FileOpsService.class).in(Singleton.class);
                        bindAsContract(FileShareService.class).in(Singleton.class);
                        bindAsContract(TenantAdminCache.class).in(Singleton.class);
                        bind(new SSHConnectionCache(CACHE_TIMEOUT_MINUTES, TimeUnit.MINUTES)).to(SSHConnectionCache.class);
                        bind(serviceClients).to(ServiceClients.class);
                        bind(tenantManager).to(TenantManager.class);
                        bind(permsService).to(FilePermsService.class);
                        bind(systemsCache).to(SystemsCache.class);
                        bindAsContract(RemoteDataClientFactory.class);
                        bind(serviceContext).to(ServiceContext.class);
                    }
                }).
                register(PostItsResource.class).
                register(OperationsApiResource.class).
                register(JWTValidateRequestFilter.class).
                register(FilePermissionsAuthz.class);
        FilePermsService.setSiteAdminTenantId("admin");
        FileShareService.setSiteAdminTenantId("admin");

        return rc;
    }

    @BeforeMethod
    public void initMocks() throws Exception
    {
        Mockito.reset(skClient, systemsCache, serviceClients, permsService);
        when(serviceClients.getClient(any(), any(), eq(SKClient.class))).thenReturn(skClient);

        // setup who is a domain admin
        when(skClient.isAdmin(any(), eq(TEST_ADMIN_USR))).thenReturn(true);
        when(skClient.isAdmin(any(), eq(TEST_USR1))).thenReturn(false);
        when(skClient.isAdmin(any(), eq(TEST_USR2))).thenReturn(false);

        // setup who will have path permission
        when(permsService.isPermitted(any(), eq(TEST_ADMIN_USR), any(), any(), any())).thenReturn(false);
        when(permsService.isPermitted(any(), eq(TEST_USR1), any(), any(), any())).thenReturn(true);
        when(permsService.isPermitted(any(), eq(TEST_USR2), any(), any(), any())).thenReturn(false);
    }

    @BeforeMethod
    @AfterMethod
    // Remove all files from test systems
    public void cleanup()
    {
        testSystems.forEach( (sys)->
        {
            try {
                when(skClient.isPermitted(any(), any(), any())).thenReturn(true);
                when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(sys);
                when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
                target(String.format("%s/%s/", OPS_ROUTE, SYSTEM_ID))
                        .request()
                        .accept(MediaType.APPLICATION_JSON)
                        .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
                        .delete();
            }
            catch (Exception ex) { log.error(ex.getMessage(), ex); }
        });
    }

    @Override
    protected void configureClient(ClientConfig config)
    {
        config.register(MultiPartFeature.class);
    }

    @DataProvider(name = "testSystemsProvider")
    public Object[][] testSystemsProvider() {
        Object[][] params = new Object[testSystems.size()][1];
        for(var i=0;i<testSystems.size();i++) {
            params[i][0] = testSystems.get(i);
        }
        return params;
    }

    @Test(dataProvider = "testSystemsProvider")
    public void testCreateAndRedeem(TapisSystem testSystem) throws Exception
    {
        // setup external service mocks
        when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);

        String fileName = "testPostItsFile.txt";
        int fileSize = 100;

        byte[] fileBytes = makeFakeFile(fileSize);
        addFile(testSystem, fileName, fileBytes);
        PostItCreateRequest request = new PostItCreateRequest();
        request.setAllowedUses(3);
        request.setValidSeconds(30);

        TapisPostItResponse postResponse = doPost(request, TEST_USR1, testSystem.getId(), fileName);
        Assert.assertNotNull(postResponse);
        Assert.assertEquals("success", postResponse.getStatus());
        Assert.assertNotNull(postResponse.getResult());
        Assert.assertNotNull(postResponse.getResult().getId());

        TapisPostItResponse getResponse = doGet(postResponse.getResult().getId());
        checkPostItsEqual(postResponse.getResult(), getResponse.getResult());
        Assert.assertEquals(getResponse.getResult().getTimesUsed(), Integer.valueOf(0));

        byte[] recievedFileBytes = doRedeem(getResponse.getResult().getRedeemUrl(), 200);
        Assert.assertArrayEquals(fileBytes, recievedFileBytes);

        // now check that use count has increased;
        getResponse = doGet(getResponse.getResult().getId());
        Assert.assertEquals(getResponse.getResult().getTimesUsed(), Integer.valueOf(1));
    }

    @Test(dataProvider = "testSystemsProvider")
    public void testCreateNotPermitted(TapisSystem testSystem) throws Exception {
        // setup external service mocks
        when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);

        String fileName = "testPostItsFile.txt";
        int fileSize = 100;

        byte[] fileBytes = makeFakeFile(fileSize);
        addFile(testSystem, fileName, fileBytes);
        PostItCreateRequest createRequest = new PostItCreateRequest();
        createRequest.setAllowedUses(3);
        createRequest.setValidSeconds(30);

        // create the postit to test
        doPost(createRequest, TEST_USR2, testSystem.getId(), fileName, 403);
    }

    @Test(dataProvider = "testSystemsProvider")
    public void testCreateBadPath(TapisSystem testSystem) throws Exception {
        // setup external service mocks
        when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);

        String fileName = "bogus.filename";
        PostItCreateRequest request = new PostItCreateRequest();
        request.setAllowedUses(3);
        request.setValidSeconds(30);

        // try with file that doesn't exit
        doPost(request, TEST_USR1, testSystem.getId(), fileName, 500);

        // try with system that doesn't exit
        doPost(request, TEST_USR1, "bogusSystem", fileName, 500);
    }

    @Test(dataProvider = "testSystemsProvider")
    public void testGetSpecialCases(TapisSystem testSystem) throws Exception {
        // setup external service mocks
        when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);

        String fileName = "testPostItsFile.txt";
        int fileSize = 100;

        byte[] fileBytes = makeFakeFile(fileSize);
        addFile(testSystem, fileName, fileBytes);
        PostItCreateRequest createRequest = new PostItCreateRequest();
        createRequest.setAllowedUses(3);
        createRequest.setValidSeconds(30);

        // create the postit to test
        TapisPostItResponse createResponse = doPost(createRequest, TEST_USR1, testSystem.getId(), fileName);
        Assert.assertNotNull(createResponse);
        String postItId = createResponse.getResult().getId();
        Assert.assertNotNull(postItId);

        // try to redeem using the wrong id
        doGet("bad_postit_id", TEST_USR1, 404);

        // try to redeem using a user that is not an owner
        doGet(postItId, TEST_USR2, 403);

        // ensure that we can read as tenant admin
        doGet(postItId, TEST_ADMIN_USR, 200);
    }


    @Test(dataProvider = "testSystemsProvider")
    public void testList(TapisSystem testSystem) throws Exception {
        // setup external service mocks
        when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);

        String fileName = "testPostItsFile.txt";
        int fileSize = 100;

        byte[] fileBytes = makeFakeFile(fileSize);
        addFile(testSystem, fileName, fileBytes);
        PostItCreateRequest request = new PostItCreateRequest();
        request.setAllowedUses(3);
        request.setValidSeconds(30);
        int usr1InitialCount = doList(TEST_USR1).getResult().size();
        int usr2InitialCount = doList(TEST_USR2).getResult().size();
        int adminUsrInitialCount = doList(TEST_ADMIN_USR).getResult().size();

        doPost(request, TEST_USR1, testSystem.getId(), fileName);
        doPost(request, TEST_USR1, testSystem.getId(), fileName);
        doPost(request, TEST_USR1, testSystem.getId(), fileName);

        TapisPostItListResponse usr1PostIts = doList(TEST_USR1);
        Assert.assertEquals(3 + usr1InitialCount, usr1PostIts.getResult().size());
        TapisPostItListResponse usr2PostIts = doList(TEST_USR2);
        Assert.assertEquals(0 + usr2InitialCount, usr2PostIts.getResult().size());
        TapisPostItListResponse adminPostIts = doList(TEST_ADMIN_USR);
        Assert.assertEquals(3 + adminUsrInitialCount, adminPostIts.getResult().size());
    }

    @Test(dataProvider = "testSystemsProvider")
    public void testUpdate(TapisSystem testSystem) throws Exception {
        // setup external service mocks
        when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);

        String fileName = "testPostItsFile.txt";
        int fileSize = 100;

        byte[] fileBytes = makeFakeFile(fileSize);
        addFile(testSystem, fileName, fileBytes);
        PostItCreateRequest createRequest = new PostItCreateRequest();
        createRequest.setAllowedUses(3);
        createRequest.setValidSeconds(30);

        // create the postit to test
        TapisPostItResponse createResponse = doPost(createRequest, TEST_USR1, testSystem.getId(), fileName);
        Assert.assertNotNull(createResponse);
        String postItId = createResponse.getResult().getId();
        Assert.assertNotNull(postItId);
        Assert.assertEquals(Integer.valueOf(3), createResponse.getResult().getAllowedUses());

        // update the postit allowedUses, get and verify
        PostItUpdateRequest updateRequest = new PostItUpdateRequest();
        updateRequest.setAllowedUses(10);
        TapisPostItResponse updateResponse = doPatch(updateRequest, postItId);
        Assert.assertNotNull(updateResponse);
        Assert.assertNotNull(updateResponse.getResult().getId());
        // should see new allowed uses, but no change to expiration
        Assert.assertEquals(Integer.valueOf(10), updateResponse.getResult().getAllowedUses());
        Assert.assertEquals(createResponse.getResult().getExpiration(), updateResponse.getResult().getExpiration());
        // now get the postit to make sure we set what the update said we set
        TapisPostItResponse getResponse = doGet(postItId);
        Assert.assertNotNull(getResponse);
        Assert.assertNotNull(getResponse.getResult().getId());
        // should see new allowed uses, but no change to expiration
        Assert.assertEquals(Integer.valueOf(10), getResponse.getResult().getAllowedUses());
        Assert.assertEquals(createResponse.getResult().getExpiration(), getResponse.getResult().getExpiration());
    }

    @Test(dataProvider = "testSystemsProvider")
    public void testRedeemUsesLimit(TapisSystem testSystem) throws Exception {
        // setup external service mocks
        when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);

        String fileName = "testPostItsFile.txt";
        int fileSize = 12;

        byte[] fileBytes = makeFakeFile(fileSize);
        addFile(testSystem, fileName, fileBytes);
        PostItCreateRequest request = new PostItCreateRequest();
        request.setAllowedUses(3);
        request.setValidSeconds(30);

        TapisPostItResponse postResponse = doPost(request, TEST_USR1, testSystem.getId(), fileName);
        Assert.assertNotNull(postResponse);
        Assert.assertNotNull(postResponse.getResult());

        PostIt newPostIt = postResponse.getResult();
        Assert.assertArrayEquals(fileBytes, doRedeem(newPostIt.getRedeemUrl(), 200));
        Assert.assertArrayEquals(fileBytes, doRedeem(newPostIt.getRedeemUrl(), 200));
        Assert.assertArrayEquals(fileBytes, doRedeem(newPostIt.getRedeemUrl(), 200));

        byte[] bytes = doRedeem(newPostIt.getRedeemUrl(), 400);
        // redeem should fail - no more allowedUsesk
        Assert.assertEquals(bytes, null);
    }

    @Test(dataProvider = "testSystemsProvider")
    public void testUnlimitedUsesLimit(TapisSystem testSystem) throws Exception {
        // setup external service mocks
        when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);

        String fileName = "testPostItsFile.txt";
        int fileSize = 12;

        byte[] fileBytes = makeFakeFile(fileSize);
        addFile(testSystem, fileName, fileBytes);
        PostItCreateRequest request = new PostItCreateRequest();
        request.setAllowedUses(-1);
        request.setValidSeconds(30);

        TapisPostItResponse postResponse = doPost(request, TEST_USR1, testSystem.getId(), fileName);
        Assert.assertNotNull(postResponse);
        Assert.assertNotNull(postResponse.getResult());

        PostIt newPostIt = postResponse.getResult();

        // can't really test unlimited, but we can test a few
        Assert.assertArrayEquals(fileBytes, doRedeem(newPostIt.getRedeemUrl(), 200));
        Assert.assertArrayEquals(fileBytes, doRedeem(newPostIt.getRedeemUrl(), 200));
        Assert.assertArrayEquals(fileBytes, doRedeem(newPostIt.getRedeemUrl(), 200));
        Assert.assertArrayEquals(fileBytes, doRedeem(newPostIt.getRedeemUrl(), 200));
    }

    @Test(dataProvider = "testSystemsProvider")
    public void redeemInvalidPostItId(TapisSystem testSystem) throws Exception {
        // setup external service mocks
        when(systemsCache.getSystem(any(), any(), any(), any(), any())).thenReturn(testSystem);

        byte[] bytes = doRedeem("v3/files/postits/redeem/49", 404);
        // redeem should fail - no more allowedUsesk
        Assert.assertEquals(bytes, null);
    }

    private byte[] doRedeem(String redeemUrl, int expectedStatus) throws IOException {
        // for test, we must strip of the host/port
        redeemUrl = redeemUrl.replaceFirst("^.*v3/", "v3/");
        Response response = target(redeemUrl).
                request().accept(MediaType.APPLICATION_JSON).
                get(Response.class);
        Assert.assertEquals(expectedStatus, response.getStatus());
        byte[] bytes = null;
        if (response.getStatus() == 200) {
            InputStream stream = response.readEntity(InputStream.class);
            bytes = stream.readAllBytes();
        }
        return bytes;
    }

    private TapisPostItResponse doGet(String postItId) {
        TapisPostItResponse getResponse = doGet(postItId, TEST_USR1, 200).
                readEntity(TapisPostItResponse.class);
        return getResponse;
    }

    private Response doGet(String postItId, String user, int expectedStatus) {
        Response getResponse = target(String.format("%s/%s", POSTITS_ROUTE, postItId)).
                request().accept(MediaType.APPLICATION_JSON).
                header("x-tapis-token", getJwtForUser(TENANT, user)).
                get();
        Assert.assertEquals(expectedStatus, getResponse.getStatus());
        return getResponse;
    }

    private TapisPostItResponse doPatch(PostItUpdateRequest updateRequest, String postItId) {
        Response response = doPatch(updateRequest, postItId, 200);
        return response.readEntity(TapisPostItResponse.class);
    }

    private Response doPatch(PostItUpdateRequest updateRequest, String postItId, int expectedStatus) {
        Response patchResponse = target(String.format("%s/%s", POSTITS_ROUTE, postItId)).
                request().accept(MediaType.APPLICATION_JSON).
                header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1)).
                build("PATCH", Entity.json(updateRequest)).
                property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true).
                invoke();
        Assert.assertEquals(expectedStatus, patchResponse.getStatus());
        return patchResponse;
    }

    private TapisPostItResponse doPost(PostItCreateRequest createRequest, String username,
                                       String systemId, String fileName) {
        return doPost(createRequest, username, systemId, fileName, 200).
                readEntity(TapisPostItResponse.class);
    }

    private Response doPost(PostItCreateRequest createRequest, String username,
                                       String systemId, String fileName, int expectedStatus) {
        Response postResponse = target(String.format("%s/%s/%s", POSTITS_ROUTE, systemId, fileName)).
                request().accept(MediaType.APPLICATION_JSON).
                header("x-tapis-token", getJwtForUser(TENANT, username)).
                post(Entity.json(createRequest));
        Assert.assertEquals(expectedStatus, postResponse.getStatus());
        return postResponse;
    }

    private TapisPostItListResponse doList(String username) {
        return doList(username, 200).readEntity(TapisPostItListResponse.class);
    }

    private Response doList(String username, int expectedStatus) {
        Response postResponse = target(String.format(POSTITS_ROUTE)).
                request().accept(MediaType.APPLICATION_JSON).
                header("x-tapis-token", getJwtForUser(TENANT, username)).
                get();
        Assert.assertEquals(expectedStatus, postResponse.getStatus());
        return postResponse;
    }

    private void checkPostItsEqual(PostIt p1, PostIt p2) {
        Assert.assertEquals(p1.getId(), p2.getId());
        Assert.assertEquals(p1.getTenantId(), p2.getTenantId());
        Assert.assertEquals(p1.getPath(), p2.getPath());
        Assert.assertEquals(p1.getOwner(), p2.getOwner());
        Assert.assertEquals(p1.getSystemId(), p2.getSystemId());
        Assert.assertEquals(p1.getJwtUser(), p2.getJwtUser());
        Assert.assertEquals(p1.getCreated(), p2.getCreated());
        Assert.assertEquals(p1.getJwtTenantId(), p2.getJwtTenantId());
        Assert.assertEquals(p1.getExpiration(), p2.getExpiration());
        Assert.assertEquals(p1.getAllowedUses(), p2.getAllowedUses());
        Assert.assertEquals(p1.getTimesUsed(), p2.getTimesUsed());
        Assert.assertEquals(p1.getUpdated(), p2.getUpdated());
        Assert.assertEquals(p1.getRedeemUrl(), p2.getRedeemUrl());
    }

    // TODO: share code with TestOpsRoutes?
    private byte[] makeFakeFile(int size)
    {
        byte[] b = new byte[size];
        new Random().nextBytes(b);
        return b;
    }

    // TODO: share code with TestOpsRoutes?
    private void addFile(TapisSystem system, String fileName, byte[] fileBytes) throws Exception {
        InputStream inputStream = new ByteArrayInputStream(fileBytes);
        File tempFile = File.createTempFile("tempfile", null);
        tempFile.deleteOnExit();
        FileOutputStream fos = new FileOutputStream(tempFile);
        fos.write(inputStream.readAllBytes());
        fos.close();
        FileDataBodyPart filePart = new FileDataBodyPart("file", tempFile);
        FormDataMultiPart form = new FormDataMultiPart();
        FormDataMultiPart multiPart = (FormDataMultiPart) form.bodyPart(filePart);
        CreateFileResponse response = target(String.format("%s/%s/%s", OPS_ROUTE, system.getId(), fileName))
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .header("x-tapis-token", getJwtForUser(TENANT, TEST_USR1))
                .post(Entity.entity(multiPart, MediaType.MULTIPART_FORM_DATA_TYPE), CreateFileResponse.class);
    }
}
