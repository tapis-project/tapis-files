package edu.utexas.tacc.tapis.files.lib.caches;

import edu.utexas.tacc.tapis.shared.ssh.apache.SSHConnection;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHExecChannel;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHScpClient;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHSftpClient;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/*
 * Class to hold state information for an SSH connection in order to manage the connection cache.
 *
 * Supports marking the connection as stale indicating it has been removed from the cache and once all users
 *   of the connection are done the connection will be stopped/closed.
 *   This allows a connection already in use by one or more clients to continue to be used until everyone is finished.
 *   Any new requests for connections will create a new connection in the cache and so any credential changes can
 *   get picked up.
 * The value for stale starts off as false and can be made true by calling makeStale().
 *   No other state changes are allowed for stale.
 */
public class SSHConnectionHolder
{
  private final SSHConnection sshConnection;
  private final AtomicLong reservationCount = new AtomicLong();
  private boolean stale = false;

  public SSHConnectionHolder(SSHConnection conn) { sshConnection = conn; }

  /*
   * Reserve the connection by atomically incrementing the reservation count.
   */
  public long reserve()
  {
    return reservationCount.incrementAndGet();
  }

  /*
   * Atomically decrement the reservation count and close the connection if the count is zero and connection is stale.
   * Note that the call to stop the connection may get called more than once.
   *  This should be OK. If it is a problem we can synchronize this method.
   */
  public long release()
  {
    long cnt = reservationCount.decrementAndGet();
    if (cnt <= 0 && stale) sshConnection.stop();
    return cnt;
  }

  /*
   * Mark the connection as stale.
   * Do a final reserve/release to make sure the connection is stopped/closed if no one is currently using it
   *
   * This method is synchronized because we do not want another thread to reserve the connection while we are
   * checking to see if it should be stopped/closed.
   */
  public synchronized void makeStale()
  {
    stale = true;
    reserve();
    release();
  }

  /*
   *  Get/Return SFTP client
   */
  public synchronized SSHSftpClient getSftpClient() throws IOException
  {
    SSHSftpClient client;
    client = sshConnection.getSftpClient();
    return client;
  }
  public synchronized void returnSftpClient(SSHSftpClient client) throws IOException
  {
    client.close();
  }

  /*
   *  Get/Return Exec Channel
   */
  public synchronized SSHExecChannel getExecChannel()
  {
    return sshConnection.getExecChannel();
  }
  public synchronized void returnExecChannel(SSHExecChannel channel) { /*execChannelSet.remove(channel);*/ }

  /*
   *  Get/Return SCP client
   */
  public synchronized SSHScpClient getScpClient() throws IOException
  {
    SSHScpClient scpClient;
    scpClient = sshConnection.getScpClient();
    return scpClient;
  }
  public synchronized void returnScpClient(SSHScpClient client) { /*scpClientSet.remove(client);*/  }

  public synchronized SSHConnection getSshConnection() { return sshConnection; }

  public long getReservationCount() { return reservationCount.longValue(); }
  public boolean isStale() { return stale; }
}
