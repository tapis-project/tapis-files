package edu.utexas.tacc.tapis.files.lib;

import edu.utexas.tacc.tapis.files.lib.cache.SSHConnectionCache;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.services.NotificationsService;
import edu.utexas.tacc.tapis.files.lib.services.TransfersService;
import edu.utexas.tacc.tapis.files.lib.transfers.ITestTransfers;
import edu.utexas.tacc.tapis.files.lib.transfers.ParentTaskFSM;
import edu.utexas.tacc.tapis.files.lib.utils.ServiceJWTCacheFactory;
import edu.utexas.tacc.tapis.files.lib.utils.SystemsClientFactory;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.sharedapi.security.ServiceJWT;
import edu.utexas.tacc.tapis.sharedapi.security.TenantManager;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;
import org.apache.commons.io.IOUtils;
import org.flywaydb.core.Flyway;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.hk2.utilities.ServiceLocatorUtilities;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.*;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;


@Test(groups={"integration"})
public abstract class BaseDatabaseIntegrationTest  {

    private static final Logger log = LoggerFactory.getLogger(BaseDatabaseIntegrationTest.class);
    protected TSystem testSystemS3;
    protected TSystem testSystemPKI;
    protected TSystem testSystemSSH;

    protected IRemoteDataClientFactory remoteDataClientFactory;
    protected ServiceLocator locator;

    protected TenantManager tenantManager = Mockito.mock(TenantManager.class);
    protected SKClient skClient = Mockito.mock(SKClient.class);
    protected SystemsClient systemsClient = Mockito.mock(SystemsClient.class);
    protected SystemsClientFactory systemsClientFactory = Mockito.mock(SystemsClientFactory.class);
    protected ServiceJWT serviceJWT;
    protected TransfersService transfersService;

    @BeforeClass
    public void initTestFixtures() throws Exception {
        String privateKey = IOUtils.toString(
            this.getClass().getResourceAsStream("/test-machine"),
            StandardCharsets.UTF_8
        );
        String publicKey = IOUtils.toString(
            this.getClass().getResourceAsStream("/test-machine.pub"),
            StandardCharsets.UTF_8
        );

        //SSH system with username/password
        Credential creds = new Credential();
        creds.setAccessKey("testuser");
        creds.setPassword("password");
        testSystemSSH = new TSystem();
        testSystemSSH.setAccessCredential(creds);
        testSystemSSH.setHost("localhost");
        testSystemSSH.setPort(2222);
        testSystemSSH.setRootDir("/data/home/testuser/");
        testSystemSSH.setName("destSystem");
        testSystemSSH.setEffectiveUserId("testuser");
        testSystemSSH.setDefaultAccessMethod(TSystem.DefaultAccessMethodEnum.PASSWORD);
        List<TSystem.TransferMethodsEnum> transferMechs = new ArrayList<>();
        transferMechs.add(TSystem.TransferMethodsEnum.SFTP);
        testSystemSSH.setTransferMethods(transferMechs);

        //S3 system
        creds = new Credential();
        creds.setAccessKey("user");
        creds.setAccessSecret("password");
        testSystemS3 = new TSystem();
        testSystemS3.setTenant("dev");
        testSystemS3.setHost("http://localhost");
        testSystemS3.setBucketName("test1");
        testSystemS3.setName("sourceSystem");
        testSystemS3.setPort(9000);
        testSystemS3.setAccessCredential(creds);
        testSystemS3.setRootDir("/");
        transferMechs = new ArrayList<>();
        transferMechs.add(TSystem.TransferMethodsEnum.S3);
        testSystemS3.setTransferMethods(transferMechs);

        // PKI Keys system
        creds = new Credential();
        creds.setPublicKey(publicKey);
        creds.setPrivateKey(privateKey);
        testSystemPKI = new TSystem();
        testSystemPKI.setAccessCredential(creds);
        testSystemPKI.setHost("localhost");
        testSystemPKI.setPort(2222);
        testSystemPKI.setRootDir("/data/home/testuser/");
        testSystemPKI.setName("testSystem");
        testSystemPKI.setEffectiveUserId("testuser");
        testSystemPKI.setDefaultAccessMethod(TSystem.DefaultAccessMethodEnum.PKI_KEYS);
        transferMechs = new ArrayList<>();
        transferMechs.add(TSystem.TransferMethodsEnum.SFTP);
        testSystemPKI.setTransferMethods(transferMechs);

        var tenant = new Tenant();
        tenant.setTenantId("testTenant");
        tenant.setBaseUrl("https://test.tapis.io");
        Map<String, Tenant> tenantMap = new HashMap<>();
        tenantMap.put(tenant.getTenantId(), tenant);
        when(tenantManager.getTenants()).thenReturn(tenantMap);
        serviceJWT = Mockito.mock(ServiceJWT.class);
        var serviceJWTFactory = Mockito.mock(ServiceJWTCacheFactory.class);
        when(serviceJWTFactory.provide()).thenReturn(serviceJWT);
        when(systemsClientFactory.getClient(any(), any())).thenReturn(systemsClient);

//        ServiceLocator locator = ServiceLocatorUtilities.createAndPopulateServiceLocator();

        locator = ServiceLocatorUtilities.bind(new AbstractBinder() {
            @Override
            protected void configure() {
                bindAsContract(ParentTaskFSM.class);
                bindAsContract(FileTransfersDAO.class);
                bindAsContract(TransfersService.class);
                bindAsContract(RemoteDataClientFactory.class);
                bindAsContract(NotificationsService.class);
                bind(new SSHConnectionCache(5, TimeUnit.MINUTES)).to(SSHConnectionCache.class);

                bind(systemsClientFactory).to(SystemsClientFactory.class);
                bind(systemsClient).to(SystemsClient.class);
                bind(tenantManager).to(TenantManager.class);
                bind(skClient).to(SKClient.class);
                bind(serviceJWTFactory).to(ServiceJWTCacheFactory.class);
                bind(serviceJWT).to(ServiceJWT.class);
                bind(tenantManager).to(TenantManager.class);
            }
        });
        remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
        transfersService = locator.getService(TransfersService.class);

    }

    @BeforeMethod
    public void doFlywayMigrations() {
        Flyway flyway = Flyway.configure()
            .dataSource("jdbc:postgresql://localhost:5432/test", "test", "test")
            .load();
        flyway.clean();
        flyway.migrate();
    }

    @BeforeTest
    public void createTestBucket() {
        Region region = Region.US_WEST_2;
        String bucket = "test";
        AwsCredentials credentials = AwsBasicCredentials.create(
            "user",
            "password"
        );
        S3Client s3 = S3Client.builder()
            .region(region)
            .credentialsProvider(StaticCredentialsProvider.create(credentials))
            .endpointOverride(URI.create("http://localhost:9000"))
            .build();

        CreateBucketRequest createBucketRequest = CreateBucketRequest
            .builder()
            .bucket(bucket)
            .createBucketConfiguration(CreateBucketConfiguration.builder()
                .locationConstraint(region.id())
                .build())
            .build();

        try {
            s3.createBucket(createBucketRequest);
        } catch (BucketAlreadyOwnedByYouException ex) {

        }
    }


}
