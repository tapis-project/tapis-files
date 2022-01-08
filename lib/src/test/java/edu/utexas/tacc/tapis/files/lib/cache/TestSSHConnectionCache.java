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
public class TestSSHConnectionCache
{
  private final TapisSystem testSystemSSH = new TapisSystem();

  public TestSSHConnectionCache()
  {
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
  // NOTE: Use calls to Thread.sleep() to simulate one scenario.
  public void testClosesConnection1() throws Exception
  {
    SSHConnectionCache cache = new SSHConnectionCache(2, TimeUnit.SECONDS);
    System.out.println("Created SSHConnectionCache");
    System.out.println("Sleeping 1 second"); Thread.sleep(1000);
    SSHConnectionHolder holder = cache.getConnection(testSystemSSH, "testuser");
    System.out.printf("ConnHolder: rsrvCount=%s stale=%s closed=%b%n", holder.getReservationCount(), holder.isStale(), holder.getSshConnection().isClosed());

    System.out.println("Reserving connection");
    holder.reserve();
    System.out.printf("ConnHolder: rsrvCount=%s stale=%s closed=%b%n", holder.getReservationCount(), holder.isStale(), holder.getSshConnection().isClosed());
    System.out.println("Sleeping 1 second"); Thread.sleep(1000);
    System.out.printf("ConnHolder: rsrvCount=%s stale=%s closed=%b%n", holder.getReservationCount(), holder.isStale(), holder.getSshConnection().isClosed());

    System.out.println("Getting sftpClient");
    SSHSftpClient client = holder.getSftpClient();
    System.out.printf("ConnHolder: rsrvCount=%s stale=%s closed=%b%n", holder.getReservationCount(), holder.isStale(), holder.getSshConnection().isClosed());
    System.out.println("Sleeping 1 second"); Thread.sleep(1000);
    System.out.printf("ConnHolder: rsrvCount=%s stale=%s closed=%b%n", holder.getReservationCount(), holder.isStale(), holder.getSshConnection().isClosed());
    System.out.println("Returning sftpClient");
    holder.returnSftpClient(client);
    System.out.println("Sleeping 1 second"); Thread.sleep(1000);
    System.out.printf("ConnHolder: rsrvCount=%s stale=%s closed=%b%n", holder.getReservationCount(), holder.isStale(), holder.getSshConnection().isClosed());

    System.out.println("Releasing connection");
    holder.release();

    // Give it a few seconds to close
    boolean closed = holder.getSshConnection().isClosed();
    for (int i = 0; i < 10; i++)
    {
      if (closed) break;
      System.out.printf("Sleeping .5 seconds - %d%n", i); Thread.sleep(500);
      System.out.printf("ConnHolder: rsrvCount=%s stale=%s closed=%b%n", holder.getReservationCount(), holder.isStale(), holder.getSshConnection().isClosed());
      closed = holder.getSshConnection().isClosed();
    }
    Assert.assertTrue(closed);
  }

  @Test
  // Test with no Thread.sleep() calls during reserve/release
  // This test is intended to check that making a connection stale does perform the final stop/close even if no other
  //   threads are using the connection.
  public void testClosesConnection2() throws Exception
  {
    SSHConnectionCache cache = new SSHConnectionCache(2, TimeUnit.SECONDS);
    System.out.println("Created SSHConnectionCache");
    SSHConnectionHolder holder = cache.getConnection(testSystemSSH, "testuser");
    System.out.printf("ConnHolder: rsrvCount=%s stale=%s closed=%b%n", holder.getReservationCount(), holder.isStale(), holder.getSshConnection().isClosed());
    System.out.println("Reserving connection");
    holder.reserve();
    System.out.printf("ConnHolder: rsrvCount=%s stale=%s closed=%b%n", holder.getReservationCount(), holder.isStale(), holder.getSshConnection().isClosed());

    System.out.println("Getting sftpClient");
    SSHSftpClient client = holder.getSftpClient();
    System.out.printf("ConnHolder: rsrvCount=%s stale=%s closed=%b%n", holder.getReservationCount(), holder.isStale(), holder.getSshConnection().isClosed());
    System.out.println("Returning sftpClient");
    holder.returnSftpClient(client);
    System.out.printf("ConnHolder: rsrvCount=%s stale=%s closed=%b%n", holder.getReservationCount(), holder.isStale(), holder.getSshConnection().isClosed());

    System.out.println("Releasing connection");
    holder.release();
    System.out.printf("ConnHolder: rsrvCount=%s stale=%s closed=%b%n", holder.getReservationCount(), holder.isStale(), holder.getSshConnection().isClosed());

    // Give it a few seconds to close
    boolean closed = holder.getSshConnection().isClosed();
    for (int i = 0; i < 10; i++)
    {
      if (closed) break;
      System.out.printf("Sleeping .5 seconds - %d%n", i); Thread.sleep(500);
      System.out.printf("ConnHolder: rsrvCount=%s stale=%s closed=%b%n", holder.getReservationCount(), holder.isStale(), holder.getSshConnection().isClosed());
      closed = holder.getSshConnection().isClosed();
    }
    Assert.assertTrue(closed);
  }
}
