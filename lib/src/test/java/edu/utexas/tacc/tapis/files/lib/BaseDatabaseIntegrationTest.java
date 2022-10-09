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
import edu.utexas.tacc.tapis.files.lib.services.FileShareService;
import edu.utexas.tacc.tapis.files.lib.services.FileUtilsService;
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
public class BaseDatabaseIntegrationTest
{
  private static final Logger log = LoggerFactory.getLogger(BaseDatabaseIntegrationTest.class);
  protected final String devTenant = "dev";
  protected final String testUser = "testuser";
  private static final String bucketName = "test-bucket";
  private static final String bucketName1 = "test-bucket1";
  private static final String bucketName2 = "test-bucket2";
  private static final String bucketName3 = "test-bucket3";

  protected TapisSystem testSystemS3, testSystemS3a, testSystemS3b, testSystemS3c;
  protected TapisSystem testSystemPKI;
  protected TapisSystem testSystemSSH, testSystemSSHa, testSystemSSHb;
  protected TapisSystem testSystemIrods, testSystemIRODSa, testSystemIRODSb;
  protected List<Pair<TapisSystem, TapisSystem>> testSystemsPairs;
  protected List<Pair<TapisSystem, TapisSystem>> testSystemsPairsNoS3;
  protected List<TapisSystem> testSystems;
  protected List<TapisSystem> testSystemsNoS3;

  protected IRemoteDataClientFactory remoteDataClientFactory;
  protected ServiceLocator locator;

  protected ServiceClients serviceClients = Mockito.mock(ServiceClients.class);
  protected FilePermsService permsService = Mockito.mock(FilePermsService.class);
  protected SystemsCache systemsCache = Mockito.mock(SystemsCache.class);

  protected TransfersService transfersService;
  protected ChildTaskTransferService childTaskTransferService;
  protected ParentTaskTransferService parentTaskTransferService;
  protected FileOpsService fileOpsService;
  protected FileUtilsService fileUtilsService;

  public BaseDatabaseIntegrationTest() throws Exception
  {
    String privateKey = IOUtils.toString(this.getClass().getResourceAsStream("/test-machine"),StandardCharsets.UTF_8);
    String publicKey = IOUtils.toString(this.getClass().getResourceAsStream("/test-machine.pub"),StandardCharsets.UTF_8);
    Credential creds = new Credential();

    //SSH systems with username/password
    creds.setPassword("password");
    testSystemSSH = new TapisSystem();
    testSystemSSH.setTenant(devTenant);
    testSystemSSH.setId("testSystemSSH");
    testSystemSSH.setSystemType(SystemTypeEnum.LINUX);
    testSystemSSH.setAuthnCredential(creds);
    testSystemSSH.setHost("localhost");
    testSystemSSH.setPort(2222);
    testSystemSSH.setRootDir("/data/home/testuser/");
    testSystemSSH.setEffectiveUserId(testUser);
    testSystemSSH.setDefaultAuthnMethod(AuthnEnum.PASSWORD);
    // =================
    creds.setPassword("password");
    testSystemSSHa = new TapisSystem();
    testSystemSSHa.setTenant(devTenant);
    testSystemSSHa.setId("testSystemSSHa");
    testSystemSSHa.setSystemType(SystemTypeEnum.LINUX);
    testSystemSSHa.setAuthnCredential(creds);
    testSystemSSHa.setHost("localhost");
    testSystemSSHa.setPort(2222);
    testSystemSSHa.setRootDir("/data/home/testuser");
    testSystemSSHa.setEffectiveUserId(testUser);
    testSystemSSHa.setDefaultAuthnMethod(AuthnEnum.PASSWORD);
    // =================
    testSystemSSHb = new TapisSystem();
    testSystemSSHb.setTenant(devTenant);
    testSystemSSHb.setId("testSystemSSHb");
    testSystemSSHb.setSystemType(SystemTypeEnum.LINUX);
    testSystemSSHb.setAuthnCredential(creds);
    testSystemSSHb.setHost("localhost");
    testSystemSSHb.setPort(2222);
    testSystemSSHb.setRootDir("/data/home/testuser");
    testSystemSSHb.setEffectiveUserId(testUser);
    testSystemSSHb.setDefaultAuthnMethod(AuthnEnum.PASSWORD);

    // PKI Keys system. The docker-compose file for development spins up 2 ssh containers,
    // one for the username/passwd system, and this separate container for the PKI ssh tests.
    creds = new Credential();
    creds.setPublicKey(publicKey);
    creds.setPrivateKey(privateKey);
    testSystemPKI = new TapisSystem();
    testSystemPKI.setTenant(devTenant);
    testSystemPKI.setId("testSystemPKI");
    testSystemPKI.setSystemType(SystemTypeEnum.LINUX);
    testSystemPKI.setAuthnCredential(creds);
    testSystemPKI.setHost("localhost");
    testSystemPKI.setPort(2223);
    testSystemPKI.setRootDir("/data/home/testuser/");
    testSystemPKI.setEffectiveUserId(testUser);
    testSystemPKI.setDefaultAuthnMethod(AuthnEnum.PKI_KEYS);

    //S3 systems
    creds = new Credential();
    creds.setAccessKey("user");
    creds.setAccessSecret("password");
    testSystemS3 = new TapisSystem();
    testSystemS3.setTenant(devTenant);
    testSystemS3.setId("testSystemS3");
    testSystemS3.setSystemType(SystemTypeEnum.S3);
    testSystemS3.setTenant(devTenant);
    testSystemS3.setHost("http://localhost");
    testSystemS3.setBucketName(bucketName);
    testSystemS3.setPort(9000);
    testSystemS3.setAuthnCredential(creds);
    testSystemS3.setDefaultAuthnMethod(AuthnEnum.ACCESS_KEY);
    // =================
    testSystemS3a = new TapisSystem();
    testSystemS3a.setTenant(devTenant);
    testSystemS3a.setId("testSystemS3a");
    testSystemS3a.setSystemType(SystemTypeEnum.S3);
    testSystemS3a.setTenant(devTenant);
    testSystemS3a.setHost("http://localhost");
    testSystemS3a.setBucketName(bucketName1);
    testSystemS3a.setPort(9000);
    testSystemS3a.setAuthnCredential(creds);
    testSystemS3a.setDefaultAuthnMethod(AuthnEnum.ACCESS_KEY);
    // =================
    testSystemS3b = new TapisSystem();
    testSystemS3b.setTenant(devTenant);
    testSystemS3b.setId("testSystemS3b");
    testSystemS3b.setSystemType(SystemTypeEnum.S3);
    testSystemS3b.setTenant(devTenant);
    testSystemS3b.setHost("http://localhost");
    testSystemS3b.setBucketName(bucketName2);
    testSystemS3b.setPort(9000);
    testSystemS3b.setAuthnCredential(creds);
    testSystemS3b.setDefaultAuthnMethod(AuthnEnum.ACCESS_KEY);
    // =================
    testSystemS3c = new TapisSystem();
    testSystemS3c.setTenant(devTenant);
    testSystemS3c.setId("testSystemS3c");
    testSystemS3c.setSystemType(SystemTypeEnum.S3);
    testSystemS3c.setTenant(devTenant);
    testSystemS3c.setHost("http://localhost");
    testSystemS3c.setBucketName(bucketName3);
    testSystemS3c.setPort(9000);
    testSystemS3c.setRootDir("/data2");
    testSystemS3c.setAuthnCredential(creds);
    testSystemS3c.setDefaultAuthnMethod(AuthnEnum.ACCESS_KEY);

    // IRODs systems
    Credential irodsCred = new Credential();
    irodsCred.setAccessKey("dev");
    irodsCred.setAccessSecret("dev");
    testSystemIrods = new TapisSystem();
    testSystemIrods.setTenant(devTenant);
    testSystemIrods.setId("testSystemIrods");
    testSystemIrods.setSystemType(SystemTypeEnum.IRODS);
    testSystemIrods.setHost("localhost");
    testSystemIrods.setPort(1247);
    testSystemIrods.setRootDir("/tempZone/home/dev/");
    testSystemIrods.setDefaultAuthnMethod(AuthnEnum.ACCESS_KEY);
    testSystemIrods.setAuthnCredential(irodsCred);
    // =================
    testSystemIRODSa = new TapisSystem();
    testSystemIRODSa.setTenant(devTenant);
    testSystemIRODSa.setId("testSystemIRODSa");
    testSystemIRODSa.setSystemType(SystemTypeEnum.IRODS);
    testSystemIRODSa.setHost("localhost");
    testSystemIRODSa.setPort(1247);
    testSystemIRODSa.setRootDir("/tempZone/home/dev/");
    testSystemIRODSa.setDefaultAuthnMethod(AuthnEnum.ACCESS_KEY);
    testSystemIRODSa.setAuthnCredential(irodsCred);
    // =================
    testSystemIRODSb = new TapisSystem();
    testSystemIRODSb.setTenant(devTenant);
    testSystemIRODSb.setId("testSystemIRODSb");
    testSystemIRODSb.setSystemType(SystemTypeEnum.IRODS);
    testSystemIRODSb.setHost("localhost");
    testSystemIRODSb.setPort(1247);
    testSystemIRODSb.setRootDir("/tempZone/home/dev/");
    testSystemIRODSb.setDefaultAuthnMethod(AuthnEnum.ACCESS_KEY);
    testSystemIRODSb.setAuthnCredential(irodsCred);

    testSystems = Arrays.asList(testSystemSSH, testSystemS3, testSystemPKI, testSystemIrods);
    testSystemsPairs = new ArrayList<>();
    for (int i = 0; i < testSystems.size(); i++)
    {
      for (int j = i + 1; j < testSystems.size(); j++)
      {
        Pair<TapisSystem, TapisSystem> pair = new ImmutablePair<>(testSystems.get(i), testSystems.get(j));
        testSystemsPairs.add(pair);
      }
    }

    testSystemsNoS3 = Arrays.asList(testSystemSSH, testSystemPKI, testSystemIrods);
    testSystemsPairsNoS3 = new ArrayList<>();
    for (int i = 0; i < testSystemsNoS3.size(); i++)
    {
      for (int j = i + 1; j < testSystemsNoS3.size(); j++)
      {
        Pair<TapisSystem, TapisSystem> pair = new ImmutablePair<>(testSystemsNoS3.get(i), testSystemsNoS3.get(j));
        testSystemsPairsNoS3.add(pair);
      }
    }
  }

  @AfterMethod
  public void resetServiceLocator()
  {
    if (locator != null) locator.shutdown();
  }

  @BeforeMethod
  public void initApplication()
  {
    ServiceContext serviceContext = Mockito.mock(ServiceContext.class);
    locator = ServiceLocatorUtilities.createAndPopulateServiceLocator();
    locator = ServiceLocatorUtilities.bind(new AbstractBinder()
    {
      @Override
      protected void configure()
      {
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
        bind(new SSHConnectionCache(5, TimeUnit.MINUTES)).to(SSHConnectionCache.class);
        bindAsContract(FileOpsService.class).in(Singleton.class);
        bindAsContract(FileShareService.class).in(Singleton.class);
        bindAsContract(FileUtilsService.class);
      }
    });
    log.info(locator.toString());
    remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
    transfersService = locator.getService(TransfersService.class);
    log.info(transfersService.toString());
    fileOpsService = locator.getService(FileOpsService.class);
    childTaskTransferService = locator.getService(ChildTaskTransferService.class);
    parentTaskTransferService = locator.getService(ParentTaskTransferService.class);
    fileUtilsService = locator.getService(FileUtilsService.class);
    FilePermsService.setSiteAdminTenantId("admin");
    FileShareService.setSiteAdminTenantId("admin");
  }

  @DataProvider
  public Object[] testSystemsDataProvider() { return testSystemsPairs.toArray(); }

  @DataProvider
  public Object[] testSystemsDataProviderNoS3() { return testSystemsPairsNoS3.toArray(); }

  @DataProvider
  public Object[] testSystemsListDataProvider() {return testSystems.toArray(); }

  @BeforeMethod
  public void doFlywayMigrations()
  {
    Flyway flyway = Flyway.configure()
            .dataSource("jdbc:postgresql://localhost:5432/test", "test", "test").load();
    flyway.clean();
    flyway.migrate();
  }

  @BeforeTest
  public void createTestBuckets()
  {
    CreateBucketRequest createBucketReq;
    Region region = Region.US_WEST_2;
    AwsCredentials credentials = AwsBasicCredentials.create("user", "password" );
    S3Client s3 = S3Client.builder().region(region).credentialsProvider(StaticCredentialsProvider.create(credentials))
            .endpointOverride(URI.create("http://localhost:9000")).build();

    createBucketReq = CreateBucketRequest.builder().bucket(bucketName)
            .createBucketConfiguration(CreateBucketConfiguration.builder().locationConstraint(region.id()).build())
            .build();
    try { s3.createBucket(createBucketReq); } catch (BucketAlreadyOwnedByYouException e) {log.warn(e.getMessage());}

    createBucketReq = CreateBucketRequest.builder().bucket(bucketName1)
            .createBucketConfiguration(CreateBucketConfiguration.builder().locationConstraint(region.id()).build())
            .build();
    try { s3.createBucket(createBucketReq); } catch (BucketAlreadyOwnedByYouException e) {log.warn(e.getMessage());}

    createBucketReq = CreateBucketRequest.builder().bucket(bucketName2)
            .createBucketConfiguration(CreateBucketConfiguration.builder().locationConstraint(region.id()).build())
            .build();
    try { s3.createBucket(createBucketReq); } catch (BucketAlreadyOwnedByYouException e) {log.warn(e.getMessage());}

    createBucketReq = CreateBucketRequest.builder().bucket(bucketName3)
            .createBucketConfiguration(CreateBucketConfiguration.builder().locationConstraint(region.id()).build())
            .build();
    try { s3.createBucket(createBucketReq); } catch (BucketAlreadyOwnedByYouException e) {log.warn(e.getMessage());}
  }
}
