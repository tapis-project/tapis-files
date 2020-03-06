package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import edu.utexas.tacc.tapis.files.lib.Utils;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@Test(groups = {"integration"})
public class ITestFileOpsServiceS3 {

    TSystem testSystem;
    // mocking out the services
    private SystemsClient systemsClient = Mockito.mock(SystemsClient.class);
    private SKClient skClient = Mockito.mock(SKClient.class);


    private ITestFileOpsServiceS3() {
        //List<String> creds = new ArrayList<>();
        Credential creds = new Credential();
        creds.setAccessKey("user");
        creds.setAccessSecret("password");
        testSystem = new TSystem();
        testSystem.setHost("http://localhost");
        testSystem.setPort(9000);
        testSystem.setBucketName("test");
        testSystem.setName("testSystem");
        testSystem.setAccessCredential(creds);
        testSystem.setRootDir("/");
        List<TSystem.TransferMethodsEnum> transferMechs = new ArrayList<>();
        transferMechs.add(TSystem.TransferMethodsEnum.S3);
        testSystem.setTransferMethods(transferMechs);
    }

    @BeforeTest
    public void setUp() {

    }

    @AfterTest
    public void tearDown() throws Exception {
        when(systemsClient.getSystemByName(any(String.class))).thenReturn(testSystem);
        FileOpsService fileOpsService = new FileOpsService(testSystem);
        fileOpsService.delete("/");
    }

    @Test
    public void testInsertAndGet() throws Exception {
        when(systemsClient.getSystemByName(any(String.class))).thenReturn(testSystem);
        FileOpsService fileOpsService = new FileOpsService(testSystem);
        InputStream in = Utils.makeFakeFile(10*1024);
        fileOpsService.insert("test.txt", in);
        InputStream out = fileOpsService.getStream("test.txt");
        Assert.assertTrue(out.readAllBytes().length == 10* 1024);
    }

    @Test
    public void testListing() throws Exception {
        when(systemsClient.getSystemByName(any(String.class))).thenReturn(testSystem);
        FileOpsService fileOpsService = new FileOpsService(testSystem);
        InputStream in = Utils.makeFakeFile(10*1024);
        fileOpsService.insert("test.txt", in);
        List<FileInfo> listing = fileOpsService.ls("/");
        Assert.assertTrue(listing.size() == 1);
        Assert.assertTrue(listing.get(0).getName().equals("test.txt"));
    }

    @Test
    public void testLargeFile() throws Exception {
        when(systemsClient.getSystemByName(any(String.class))).thenReturn(testSystem);
        FileOpsService fileOpsService = new FileOpsService(testSystem);
        InputStream in = Utils.makeFakeFile(100 * 1000 * 1024);
        fileOpsService.insert("test.txt", in);
        List<FileInfo> listing = fileOpsService.ls("/");
        Assert.assertTrue(listing.size() == 1);
        Assert.assertTrue(listing.get(0).getName().equals("test.txt"));
        Assert.assertEquals(listing.get(0).getSize(), 100 * 1000 * 1024L);
    }

}
