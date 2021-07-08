package edu.utexas.tacc.tapis.files.lib.cache;

import edu.utexas.tacc.tapis.files.lib.caches.SSHConnectionCache;
import edu.utexas.tacc.tapis.files.lib.caches.SSHConnectionHolder;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHSftpClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.AuthnEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

@Test(groups = {"integration"})
public class ITestSSHConnectionCache {

    private final TapisSystem testSystemSSH = new TapisSystem();

    public ITestSSHConnectionCache() {
        //SSH system with username/password
        Credential creds = new Credential();
        creds.setAccessKey("testuser");
        creds.setPassword("password");
        testSystemSSH.setSystemType(SystemTypeEnum.LINUX);
        testSystemSSH.setAuthnCredential(creds);
        testSystemSSH.setHost("localhost");
        testSystemSSH.setPort(2222);
        testSystemSSH.setRootDir("/data/home/testuser/");
        testSystemSSH.setId("testSystem");
        testSystemSSH.setDefaultAuthnMethod(AuthnEnum.PASSWORD);
        testSystemSSH.setEffectiveUserId("testuser");
    }

    @Test
    public void testClosesConnection() throws Exception {
        SSHConnectionCache cache = new SSHConnectionCache(2, TimeUnit.SECONDS);
        SSHConnectionHolder holder = cache.getConnection(testSystemSSH, "testuser");
        SSHSftpClient client = holder.getSftpClient();
        holder.returnSftpClient(client);
        Thread.sleep(2000);
        Assert.assertTrue(holder.getSshConnection().isClosed());

    }


}
