package edu.utexas.tacc.tapis.files.lib;

import edu.utexas.tacc.tapis.files.lib.caches.FilePermsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.factories.ServiceContextFactory;
import edu.utexas.tacc.tapis.files.lib.services.ChildTaskTransferService;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService;
import edu.utexas.tacc.tapis.files.lib.services.FilePermsService;
import edu.utexas.tacc.tapis.files.lib.services.IFileOpsService;
import edu.utexas.tacc.tapis.files.lib.providers.ServiceClientsFactory;
import edu.utexas.tacc.tapis.files.lib.providers.TenantCacheFactory;
import edu.utexas.tacc.tapis.files.lib.services.ParentTaskTransferService;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.shared.ssh.SSHConnectionCache;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.services.TransfersService;
import edu.utexas.tacc.tapis.files.lib.providers.ServiceJWTCacheFactory;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.security.ServiceJWT;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.AuthnEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
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

import javax.inject.Singleton;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;


@Test(groups={"integration"})
public abstract class BaseDatabaseIntegrationTest  {

    private static final Logger log = LoggerFactory.getLogger(BaseDatabaseIntegrationTest.class);
    protected TapisSystem testSystemS3;
    protected TapisSystem testSystemPKI;
    protected TapisSystem testSystemSSH;

    protected IRemoteDataClientFactory remoteDataClientFactory;
    protected ServiceLocator locator;

    protected SKClient skClient = Mockito.mock(SKClient.class);
    protected SystemsClient systemsClient = Mockito.mock(SystemsClient.class);
    protected ServiceClients serviceClients = Mockito.mock(ServiceClients.class);
    protected FilePermsService permsService = Mockito.mock(FilePermsService.class);

    protected TransfersService transfersService;
    protected ChildTaskTransferService childTaskTransferService;
    protected ParentTaskTransferService parentTaskTransferService;
    protected IFileOpsService fileOpsService;

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
        testSystemSSH = new TapisSystem();
        testSystemSSH.setSystemType(SystemTypeEnum.LINUX);
        testSystemSSH.setAuthnCredential(creds);
        testSystemSSH.setHost("localhost");
        testSystemSSH.setPort(2222);
        testSystemSSH.setRootDir("/data/home/testuser/");
        testSystemSSH.setId("destSystem");
        testSystemSSH.setEffectiveUserId("testuser");
        testSystemSSH.setDefaultAuthnMethod(AuthnEnum.PASSWORD);

        //S3 system
        creds = new Credential();
        creds.setAccessKey("user");
        creds.setAccessSecret("password");
        testSystemS3 = new TapisSystem();
        testSystemS3.setSystemType(SystemTypeEnum.S3);
        testSystemS3.setTenant("dev");
        testSystemS3.setHost("http://localhost");
        testSystemS3.setBucketName("test");
        testSystemS3.setId("sourceSystem");
        testSystemS3.setPort(9000);
        testSystemS3.setAuthnCredential(creds);
        testSystemS3.setRootDir("/");
        testSystemS3.setDefaultAuthnMethod(AuthnEnum.ACCESS_KEY);

        // PKI Keys system
        creds = new Credential();
        creds.setPublicKey(publicKey);
        creds.setPrivateKey(privateKey);
        testSystemPKI = new TapisSystem();
        testSystemPKI.setSystemType(SystemTypeEnum.LINUX);
        testSystemPKI.setAuthnCredential(creds);
        testSystemPKI.setHost("localhost");
        testSystemPKI.setPort(2222);
        testSystemPKI.setRootDir("/data/home/testuser/");
        testSystemPKI.setId("testSystem");
        testSystemPKI.setEffectiveUserId("testuser");
        testSystemPKI.setDefaultAuthnMethod(AuthnEnum.PKI_KEYS);
        ServiceContext serviceContext = Mockito.mock(ServiceContext.class);


        locator = ServiceLocatorUtilities.bind(new AbstractBinder() {
            @Override
            protected void configure() {
            bindFactory(TenantCacheFactory.class).to(TenantManager.class).in(Singleton.class);
            bind(new SSHConnectionCache(5, TimeUnit.MINUTES)).to(SSHConnectionCache.class);
            bindAsContract(SystemsCache.class).in(Singleton.class);
            bindAsContract(TransfersService.class).in(Singleton.class);
            bindAsContract(ChildTaskTransferService.class).in(Singleton.class);
            bindAsContract(ParentTaskTransferService.class).in(Singleton.class);
            bindAsContract(FileTransfersDAO.class);
            bind(permsService).to(FilePermsService.class);
            bind(serviceClients).to(ServiceClients.class);
            bind(serviceContext).to(ServiceContext.class);
            bindAsContract(RemoteDataClientFactory.class);
            bind(FileOpsService.class).to(IFileOpsService.class).in(Singleton.class);
            }
        });
        remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
        transfersService = locator.getService(TransfersService.class);
        fileOpsService = locator.getService(IFileOpsService.class);
        childTaskTransferService = locator.getService(ChildTaskTransferService.class);
        parentTaskTransferService = locator.getService(ParentTaskTransferService.class);
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
