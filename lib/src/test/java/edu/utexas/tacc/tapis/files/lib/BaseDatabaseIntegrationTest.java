package edu.utexas.tacc.tapis.files.lib;

import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCacheNoAuth;
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
import edu.utexas.tacc.tapis.security.client.SKClient;
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


@Test(groups={"integration"})
public class BaseDatabaseIntegrationTest
{
  private static final Logger log = LoggerFactory.getLogger(BaseDatabaseIntegrationTest.class);
  protected final String devTenant = "dev";
  protected final String testUser = "testuser";
  private static final String bucketName = "test-bucket";
  private static final String bucketNameA = "test-bucket1";
  private static final String bucketNameB = "test-bucket2";
  private static final String bucketNameC = "test-bucket3";

  protected TapisSystem testSystemS3, testSystemS3a, testSystemS3b, testSystemS3c, testSystemS3BucketC;
  protected TapisSystem testSystemPKI;
  protected TapisSystem testSystemSSH, testSystemSSHa, testSystemSSHb;
  protected TapisSystem testSystemIrods, testSystemIRODSa, testSystemIRODSb;
  protected List<Pair<TapisSystem, TapisSystem>> testSystemsPairs;
  protected List<Pair<TapisSystem, TapisSystem>> testSystemsPairs1;
  protected List<Pair<TapisSystem, TapisSystem>> testSystemsPairsNoS3;
  protected List<TapisSystem> testSystems;
  protected List<TapisSystem> testSystems1;
  protected List<TapisSystem> testSystemsNoS3;

  protected IRemoteDataClientFactory remoteDataClientFactory;
  protected ServiceLocator locator;

  protected ServiceClients serviceClients = Mockito.mock(ServiceClients.class);
  protected SKClient skClient = Mockito.mock(SKClient.class);
  protected FilePermsService permsService = Mockito.mock(FilePermsService.class);
  protected SystemsCache systemsCache = Mockito.mock(SystemsCache.class);
  protected SystemsCacheNoAuth systemsCacheNoAuth = Mockito.mock(SystemsCacheNoAuth.class);

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
    testSystemS3a.setHost("http://localhost");
    testSystemS3a.setBucketName(bucketNameA);
    testSystemS3a.setPort(9000);
    testSystemS3a.setAuthnCredential(creds);
    testSystemS3a.setDefaultAuthnMethod(AuthnEnum.ACCESS_KEY);
    // =================
    testSystemS3b = new TapisSystem();
    testSystemS3b.setTenant(devTenant);
    testSystemS3b.setId("testSystemS3b");
    testSystemS3b.setSystemType(SystemTypeEnum.S3);
    testSystemS3b.setHost("http://localhost");
    testSystemS3b.setBucketName(bucketNameB);
    testSystemS3b.setPort(9000);
    testSystemS3b.setAuthnCredential(creds);
    testSystemS3b.setDefaultAuthnMethod(AuthnEnum.ACCESS_KEY);
    // =================
    testSystemS3c = new TapisSystem();
    testSystemS3c.setTenant(devTenant);
    testSystemS3c.setId("testSystemS3c");
    testSystemS3c.setSystemType(SystemTypeEnum.S3);
    testSystemS3c.setHost("http://localhost");
    testSystemS3c.setBucketName(bucketNameC);
    testSystemS3c.setPort(9000);
    testSystemS3c.setRootDir("data2");
    testSystemS3c.setAuthnCredential(creds);
    testSystemS3c.setDefaultAuthnMethod(AuthnEnum.ACCESS_KEY);
    // ================= Bucket to see all keys in bucketNameC
    testSystemS3BucketC = new TapisSystem();
    testSystemS3BucketC.setTenant(devTenant);
    testSystemS3BucketC.setId("testSystemS3BucketC");
    testSystemS3BucketC.setSystemType(SystemTypeEnum.S3);
    testSystemS3BucketC.setHost("http://localhost");
    testSystemS3BucketC.setBucketName(bucketNameC);
    testSystemS3BucketC.setPort(9000);
    testSystemS3BucketC.setRootDir("");
    testSystemS3BucketC.setAuthnCredential(creds);
    testSystemS3BucketC.setDefaultAuthnMethod(AuthnEnum.ACCESS_KEY);

    // IRODs systems
    Credential irodsCred = new Credential();
    irodsCred.setLoginUser("dev");
    irodsCred.setPassword("dev");
    testSystemIrods = new TapisSystem();
    testSystemIrods.setTenant(devTenant);
    testSystemIrods.setId("testSystemIrods");
    testSystemIrods.setSystemType(SystemTypeEnum.IRODS);
    testSystemIrods.setHost("localhost");
    testSystemIrods.setPort(1247);
    testSystemIrods.setRootDir("/tempZone/home/dev/");
    testSystemIrods.setDefaultAuthnMethod(AuthnEnum.PASSWORD);
    testSystemIrods.setAuthnCredential(irodsCred);
    testSystemIrods.setEffectiveUserId(irodsCred.getLoginUser());
    // =================
    testSystemIRODSa = new TapisSystem();
    testSystemIRODSa.setTenant(devTenant);
    testSystemIRODSa.setId("testSystemIRODSa");
    testSystemIRODSa.setSystemType(SystemTypeEnum.IRODS);
    testSystemIRODSa.setHost("localhost");
    testSystemIRODSa.setPort(1247);
    testSystemIRODSa.setRootDir("/tempZone/home/dev/");
    testSystemIRODSa.setDefaultAuthnMethod(AuthnEnum.PASSWORD);
    testSystemIRODSa.setAuthnCredential(irodsCred);
    testSystemIRODSa.setEffectiveUserId(irodsCred.getLoginUser());
    // =================
    testSystemIRODSb = new TapisSystem();
    testSystemIRODSb.setTenant(devTenant);
    testSystemIRODSb.setId("testSystemIRODSb");
    testSystemIRODSb.setSystemType(SystemTypeEnum.IRODS);
    testSystemIRODSb.setHost("localhost");
    testSystemIRODSb.setPort(1247);
    testSystemIRODSb.setRootDir("/tempZone/home/dev/");
    testSystemIRODSb.setDefaultAuthnMethod(AuthnEnum.PASSWORD);
    testSystemIRODSb.setAuthnCredential(irodsCred);
    testSystemIRODSb.setEffectiveUserId(irodsCred.getLoginUser());

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

    testSystems1 = Arrays.asList(testSystemSSH, testSystemPKI);
    testSystemsPairs1 = new ArrayList<>();
    for (int i = 0; i < testSystems1.size(); i++)
    {
      for (int j = i + 1; j < testSystems1.size(); j++)
      {
        Pair<TapisSystem, TapisSystem> pair = new ImmutablePair<>(testSystems1.get(i), testSystems1.get(j));
        testSystemsPairs1.add(pair);
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
        bind(systemsCacheNoAuth).to(SystemsCacheNoAuth.class);
        bindAsContract(TransfersService.class).in(Singleton.class);
        bindAsContract(ChildTaskTransferService.class).in(Singleton.class);
        bindAsContract(ParentTaskTransferService.class).in(Singleton.class);
        bindAsContract(FileTransfersDAO.class);
        bind(permsService).to(FilePermsService.class);
        bind(serviceClients).to(ServiceClients.class);
        bind(serviceContext).to(ServiceContext.class);
        bindAsContract(RemoteDataClientFactory.class);
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
  public Object[] testSystemsDataProvider1() { return testSystemsPairs1.toArray(); }

  @DataProvider
  public Object[] testSystemsDataProviderNoS3() { return testSystemsPairsNoS3.toArray(); }

  @DataProvider
  public Object[] testSystemsListDataProvider() {return testSystems.toArray(); }

  @BeforeMethod
  public void doFlywayMigrations()
  {
    Flyway flyway = Flyway.configure()
        .cleanDisabled(false)
        .dataSource("jdbc:postgresql://localhost:5432/test", "test", "test")
        .load();
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

    createBucketReq = CreateBucketRequest.builder().bucket(bucketNameA)
            .createBucketConfiguration(CreateBucketConfiguration.builder().locationConstraint(region.id()).build())
            .build();
    try { s3.createBucket(createBucketReq); } catch (BucketAlreadyOwnedByYouException e) {log.warn(e.getMessage());}

    createBucketReq = CreateBucketRequest.builder().bucket(bucketNameB)
            .createBucketConfiguration(CreateBucketConfiguration.builder().locationConstraint(region.id()).build())
            .build();
    try { s3.createBucket(createBucketReq); } catch (BucketAlreadyOwnedByYouException e) {log.warn(e.getMessage());}

    createBucketReq = CreateBucketRequest.builder().bucket(bucketNameC)
            .createBucketConfiguration(CreateBucketConfiguration.builder().locationConstraint(region.id()).build())
            .build();
    try { s3.createBucket(createBucketReq); } catch (BucketAlreadyOwnedByYouException e) {log.warn(e.getMessage());}
  }
}
