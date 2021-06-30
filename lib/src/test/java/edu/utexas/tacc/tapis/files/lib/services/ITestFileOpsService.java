package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.Utils;
import edu.utexas.tacc.tapis.files.lib.caches.FilePermsCache;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.ssh.SSHConnectionCache;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.systems.client.gen.model.AuthnEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import jdk.jshell.execution.Util;
import org.apache.sshd.sftp.client.SftpClient;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.hk2.utilities.ServiceLocatorUtilities;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.apache.commons.io.IOUtils;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.*;

import javax.inject.Singleton;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/*
 These tests are designed to run against the ssh-machine image in the service's docker-compose file.
 The user is set to be testuser
*/
@Test(groups = {"integration"})
public class ITestFileOpsService {

    private final String oboTenant = "oboTenant";
    private final String oboUser = "oboUser";
    TapisSystem testSystemSSH;
    TapisSystem testSystemS3;
    TapisSystem testSystemPKI;
    private RemoteDataClientFactory remoteDataClientFactory;
    private IFileOpsService fileOpsService;
    private static final Logger log  = LoggerFactory.getLogger(ITestFileOpsService.class);
    private final FilePermsService permsService = Mockito.mock(FilePermsService.class);

    private ITestFileOpsService() throws IOException {
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
        testSystemSSH.setId("testSystem");
        testSystemSSH.setDefaultAuthnMethod(AuthnEnum.PASSWORD);
        testSystemSSH.setEffectiveUserId("testuser");

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
        testSystemPKI.setDefaultAuthnMethod(AuthnEnum.PKI_KEYS);
        testSystemPKI.setEffectiveUserId("testuser");

        //S3 system
        creds = new Credential();
        creds.setAccessKey("user");
        creds.setAccessSecret("password");
        testSystemS3 = new TapisSystem();
        testSystemS3.setSystemType(SystemTypeEnum.S3);
        testSystemS3.setHost("http://localhost");
        testSystemS3.setBucketName("test");
        testSystemS3.setId("testSystem");
        testSystemS3.setPort(9000);
        testSystemS3.setAuthnCredential(creds);
        testSystemS3.setRootDir("/");
        testSystemS3.setDefaultAuthnMethod(AuthnEnum.ACCESS_KEY);
    }

    @DataProvider(name="testSystems")
    public Object[] testSystemsDataProvider () {
        return new TapisSystem[]{
                testSystemSSH,
                testSystemPKI,
                testSystemS3
        };
    }

    @BeforeSuite
    public void doBeforeSuite() {
        ServiceLocator locator = ServiceLocatorUtilities.createAndPopulateServiceLocator();
        ServiceLocatorUtilities.bind(locator, new AbstractBinder() {
            @Override
            protected void configure() {
                bind(new SSHConnectionCache(5, TimeUnit.MINUTES)).to(SSHConnectionCache.class);
                bindAsContract(RemoteDataClientFactory.class).in(Singleton.class);
                bind(permsService).to(FilePermsService.class).ranked(1);
            }
        });
        remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
        fileOpsService = locator.getService(FileOpsService.class);
    }

    @BeforeTest()
    public void setUp() throws Exception {

    }

    @AfterTest()
    public void tearDown() throws Exception {
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystemSSH, "testuser");
        fileOpsService.delete(client,"/");
        client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystemS3, "testuser");
        fileOpsService.delete(client, "/");
    }

    @Test(dataProvider = "testSystems")
    public void testInsertAndDelete(TapisSystem testSystem) throws Exception {
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystem, "testuser");
        InputStream in = Utils.makeFakeFile(10*1024);
        fileOpsService.insert(client,"test.txt", in);
        List<FileInfo> listing = fileOpsService.ls(client,"test.txt");
        Assert.assertEquals(listing.size(), 1);
        fileOpsService.delete(client,"/test.txt");
        Assert.assertThrows(NotFoundException.class, ()-> {
            fileOpsService.ls(client, "/test.txt");
        });
    }

    @Test(dataProvider = "testSystems")
    public void testInsertAndDeleteNested(TapisSystem testSystem) throws Exception {
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystem, "testuser");
        InputStream in = Utils.makeFakeFile(10*1024);
        fileOpsService.insert(client,"a/b/c/test.txt", in);
        List<FileInfo> listing = fileOpsService.ls(client,"/a/b/c/test.txt");
        Assert.assertEquals(listing.size(), 1);
        fileOpsService.delete(client,"/a/b/");
        Assert.assertThrows(NotFoundException.class, ()-> {
            fileOpsService.ls(client,"/a/b/c/test.txt");
        });
    }

    @Test(dataProvider = "testSystems")
    public void testInsertAndGet(TapisSystem testSystem) throws Exception {
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystem, "testuser");
        InputStream in = Utils.makeFakeFile(10*1024);
        fileOpsService.insert(client,"test.txt", in);
        InputStream out = fileOpsService.getStream(client,"test.txt");
        Assert.assertEquals(out.readAllBytes().length,10* 1024);
        out.close();
    }


    @Test(dataProvider = "testSystems")
    public void testListing(TapisSystem testSystem) throws Exception {
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystem, "testuser");
        InputStream in = Utils.makeFakeFile(10*1024);
        fileOpsService.insert(client,"test.txt", in);
        List<FileInfo> listing = fileOpsService.ls(client,"test.txt");
        Assert.assertEquals(listing.size(), 1);
        Assert.assertEquals(listing.get(0).getName(), "test.txt");

    }

    @Test(dataProvider = "testSystems")
    public void testInsertLargeFile(TapisSystem testSystem) throws Exception {
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystem, "testuser");
        InputStream in = Utils.makeFakeFile(100 * 1000 * 1024);
        fileOpsService.insert(client,"test.txt", in);
        List<FileInfo> listing = fileOpsService.ls(client,"test.txt");
        Assert.assertEquals(listing.size(), 1);
        Assert.assertEquals(listing.get(0).getName(), "test.txt");
        Assert.assertEquals(listing.get(0).getSize(), 100 * 1000 * 1024L);
    }

    @Test(dataProvider = "testSystems")
    public void testGetBytesByRange(TapisSystem testSystem) throws Exception {
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystem, "testuser");
        InputStream in = Utils.makeFakeFile( 1000 * 1024);
        fileOpsService.insert(client,"test.txt", in);
        InputStream result = fileOpsService.getBytes(client,"test.txt", 0, 1000);
        Assert.assertEquals(result.readAllBytes().length, 1000);
    }

    @Test(dataProvider = "testSystems")
    public void testZipFolder(TapisSystem testSystem) throws Exception {
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystem, "testuser");
        log.info(client.ls("/").toString());
        client.delete("/");
        fileOpsService.insert(client,"a/test1.txt", Utils.makeFakeFile( 1000 * 1024));
        fileOpsService.insert(client,"a/test2.txt", Utils.makeFakeFile(1000 * 1024));
        fileOpsService.insert(client,"a/b/test3.txt", Utils.makeFakeFile(1000 * 1024));

        File file = File.createTempFile("test", ".zip");
        OutputStream outputStream = new FileOutputStream(file);
        fileOpsService.getZip(client, outputStream, "/a");

        try (FileInputStream fis = new FileInputStream(file);
             ZipInputStream zis = new ZipInputStream(fis);
        ) {
            ZipEntry ze;
            int count = 0;
            while ( (ze = zis.getNextEntry()) != null) {
                log.info(ze.toString());
                String fname = ze.getName();
                count++;
            }
            Assert.assertEquals(count, 3);
        }
        file.deleteOnExit();
    }


    @Test(dataProvider = "testSystems")
    public void testInsertNoAuthz(TapisSystem testSystem) throws Exception {
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(false);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystem, "testuser");
        InputStream in = Utils.makeFakeFile(10*1024);

        Assert.assertThrows(ForbiddenException.class, ()-> {
            fileOpsService.insert(client,"test.txt", in);
        });
    }

    @Test(dataProvider = "testSystems")
    public void testListingNoAuthz(TapisSystem testSystem) throws Exception {
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(false);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystem, "testuser");
        Assert.assertThrows(ForbiddenException.class, ()-> {
            fileOpsService.ls(client,"test.txt");
        });

    }

    @Test(dataProvider = "testSystems")
    public void testListingRecursive(TapisSystem testSystem) throws Exception {
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystem, "testuser");
        client.delete("/");
        fileOpsService.insert(client,"/1.txt", Utils.makeFakeFile(10*1024));
        fileOpsService.insert(client,"/a/2.txt", Utils.makeFakeFile(10*1024));
        fileOpsService.insert(client,"/a/b/3.txt", Utils.makeFakeFile(10*1024));
        fileOpsService.insert(client,"/a/b/c/4.txt", Utils.makeFakeFile(10*1024));

        List<FileInfo> listing = fileOpsService.lsRecursive(client,"/", 5);
        log.info(listing.toString());
        Assert.assertEquals(listing.size(), 4);

    }


}
