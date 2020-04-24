package edu.utexas.tacc.tapis.files.lib.cache;

import edu.utexas.tacc.tapis.files.lib.kernel.SSHConnection;
import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

@Test
public class TestSSHConnectionCache {

    TSystem testSystem;

    @BeforeTest
    public void setUp() {
        Credential creds = new Credential();
        creds.setAccessKey("testuser");
        creds.setPassword("password");
        testSystem = new TSystem();
        testSystem.setAccessCredential(creds);
        testSystem.setHost("localhost");
        testSystem.setPort(2222);
        testSystem.setRootDir("/home/testuser/");
        testSystem.setName("testSystem");
        testSystem.setEffectiveUserId("testuser");
        testSystem.setDefaultAccessMethod(TSystem.DefaultAccessMethodEnum.PASSWORD);
        List<TSystem.TransferMethodsEnum> transferMechs = new ArrayList<>();
        transferMechs.add(TSystem.TransferMethodsEnum.SFTP);
        testSystem.setTransferMethods(transferMechs);
    }


    @Test
    public void testCacheGet() throws Exception {
        SSHConnectionCache cache = new SSHConnectionCache(1);

        SSHConnection test = cache.getConnection(testSystem, "testuser");
        Assert.assertNotNull(test);
        cache.getCache().invalidate(new SSHConnectionCacheKey(testSystem, "testuser"));
        Thread.sleep(2000);
        Assert.assertFalse(test.getSession().isConnected());

    }



}
