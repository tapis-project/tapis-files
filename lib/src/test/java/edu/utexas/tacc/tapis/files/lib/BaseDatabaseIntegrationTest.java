package edu.utexas.tacc.tapis.files.lib;

import edu.utexas.tacc.tapis.files.lib.caches.SSHConnectionCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.providers.TenantCacheFactory;
import edu.utexas.tacc.tapis.files.lib.services.ChildTaskTransferService;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService;
import edu.utexas.tacc.tapis.files.lib.services.FilePermsService;
import edu.utexas.tacc.tapis.files.lib.services.FileUtilsService;
import edu.utexas.tacc.tapis.files.lib.services.IFileOpsService;
import edu.utexas.tacc.tapis.files.lib.services.ParentTaskTransferService;
import edu.utexas.tacc.tapis.files.lib.services.TransfersService;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.systems.client.gen.model.AuthnEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.flywaydb.core.Flyway;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.hk2.utilities.ServiceLocatorUtilities;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;


@Test(groups={"integration"})
public class BaseDatabaseIntegrationTest  {

    private static final Logger log = LoggerFactory.getLogger(BaseDatabaseIntegrationTest.class);
    protected TapisSystem testSystemS3;
    protected TapisSystem testSystemPKI;
    protected TapisSystem testSystemSSH;
    protected TapisSystem testSystemIrods;
    protected List<Pair<TapisSystem, TapisSystem>> testSystemsPairs = new ArrayList<>();
    protected List<TapisSystem> testSystems = new ArrayList<>();

    protected IRemoteDataClientFactory remoteDataClientFactory;
    protected ServiceLocator locator;

    protected ServiceClients serviceClients = Mockito.mock(ServiceClients.class);
    protected FilePermsService permsService = Mockito.mock(FilePermsService.class);
    protected SystemsCache systemsCache = Mockito.mock(SystemsCache.class);

    protected TransfersService transfersService;
    protected ChildTaskTransferService childTaskTransferService;
    protected ParentTaskTransferService parentTaskTransferService;
    protected IFileOpsService fileOpsService;
    protected FileUtilsService fileUtilsService;

    public BaseDatabaseIntegrationTest() throws Exception {
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
        testSystemSSH.setId("testSystemSSH");
        testSystemSSH.setEffectiveUserId("testuser");
        testSystemSSH.setDefaultAuthnMethod(AuthnEnum.PASSWORD);


        // PKI Keys system. The docker-compose file for development spins up
        // 2 ssh containers, one for the username/passwd system, and this separate container
        // for the PKI ssh tests.
        creds = new Credential();
        creds.setPublicKey(publicKey);
        creds.setPrivateKey(privateKey);
        testSystemPKI = new TapisSystem();
        testSystemPKI.setSystemType(SystemTypeEnum.LINUX);
        testSystemPKI.setAuthnCredential(creds);
        testSystemPKI.setHost("localhost");
        testSystemPKI.setPort(2223);
        testSystemPKI.setRootDir("/data/home/testuser/");
        testSystemPKI.setId("testSystemPKI");
        testSystemPKI.setEffectiveUserId("testuser");
        testSystemPKI.setDefaultAuthnMethod(AuthnEnum.PKI_KEYS);

        //S3 system
        creds = new Credential();
        creds.setAccessKey("user");
        creds.setAccessSecret("password");
        testSystemS3 = new TapisSystem();
        testSystemS3.setSystemType(SystemTypeEnum.S3);
        testSystemS3.setTenant("dev");
        testSystemS3.setHost("http://localhost");
        testSystemS3.setBucketName("test");
        testSystemS3.setId("testSystemS3");
        testSystemS3.setPort(9000);
        testSystemS3.setAuthnCredential(creds);
        testSystemS3.setRootDir("/");
        testSystemS3.setDefaultAuthnMethod(AuthnEnum.ACCESS_KEY);

        // IRODs system
        testSystemIrods = new TapisSystem();
        testSystemIrods.setSystemType(SystemTypeEnum.IRODS);
        testSystemIrods.setId("testSystemIrods");
        testSystemIrods.setHost("localhost");
        testSystemIrods.setPort(1247);
        testSystemIrods.setRootDir("/tempZone/home/dev/");
        testSystemIrods.setDefaultAuthnMethod(AuthnEnum.ACCESS_KEY);
        creds = new Credential();
        creds.setAccessKey("dev");
        creds.setAccessSecret("dev");
        testSystemIrods.setAuthnCredential(creds);

        testSystems = Arrays.asList(testSystemSSH, testSystemS3, testSystemPKI, testSystemIrods);
        testSystemsPairs = new ArrayList<>();

        for (int i = 0; i < testSystems.size(); i++) {
            for (int j = i + 1; j < testSystems.size(); j++) {
                Pair<TapisSystem, TapisSystem> pair = new ImmutablePair<>(testSystems.get(i), testSystems.get(j));
                testSystemsPairs.add(pair);
            }
        }
    }

    @AfterMethod
    public void resetServiceLocator() {
        if (locator != null) {
            locator.shutdown();
        }
    }

    @BeforeMethod
    public void initApplication() {
        ServiceContext serviceContext = Mockito.mock(ServiceContext.class);
        locator = ServiceLocatorUtilities.createAndPopulateServiceLocator();
        locator = ServiceLocatorUtilities.bind(new AbstractBinder() {
            @Override
            protected void configure() {
            bindFactory(TenantCacheFactory.class).to(TenantManager.class).in(Singleton.class);
            bind(systemsCache).to(SystemsCache.class);
            bindAsContract(TransfersService.class).in(Singleton.class);
            bindAsContract(ChildTaskTransferService.class).in(Singleton.class);
            bindAsContract(ParentTaskTransferService.class).in(Singleton.class);
            bindAsContract(FileTransfersDAO.class);
            bind(permsService).to(FilePermsService.class);
            bind(serviceClients).to(ServiceClients.class);
            bind(serviceContext).to(ServiceContext.class);
            bindAsContract(RemoteDataClientFactory.class);
            bindAsContract(FileUtilsService.class);
            bind(new SSHConnectionCache(5, TimeUnit.MINUTES)).to(SSHConnectionCache.class);
            bind(FileOpsService.class).to(IFileOpsService.class).in(Singleton.class);
            }
        });
        log.info(locator.toString());
        remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
        transfersService = locator.getService(TransfersService.class);
        log.info(transfersService.toString());
        fileOpsService = locator.getService(IFileOpsService.class);
        childTaskTransferService = locator.getService(ChildTaskTransferService.class);
        parentTaskTransferService = locator.getService(ParentTaskTransferService.class);
        fileUtilsService = locator.getService(FileUtilsService.class);
    }

    @DataProvider
    public Object[] testSystemsDataProvider() {
        return testSystemsPairs.toArray();
    }

    @DataProvider
    public Object[] testSystemsListDataProvider() {return testSystems.toArray(); }


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
