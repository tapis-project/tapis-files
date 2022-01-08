package edu.utexas.tacc.tapis.files.lib.caches;

import edu.utexas.tacc.tapis.shared.ssh.apache.SSHConnection;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHExecChannel;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHScpClient;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHSftpClient;
import org.apache.sshd.client.session.ClientSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/*
 * Class to hold state information for an SSH connection in order to manage the connection cache.
 *
 * Supports marking the connection as stale indicating it has been removed from the cache and once all users
 *   of the connection are done the connection will be closed.
 *   This allows a connection already in use by one or more clients to continue to be used until everyone is finished.
 *   Any new requests for connections will create a new connection in the cache and so any credential changes can
 *   get picked up.
 * The value for stale starts off as false and can be made true by calling makeStale(). No other state changes.
 */
public class SSHConnectionHolder
{
//  private final Logger log = LoggerFactory.getLogger(SSHConnectionHolder.class);
  private final SSHConnection sshConnection;
// TODO/TBD: With updates to the cache and how expiration and connection close is handled do we still need to keep
//  track of the channels that are created as the connection is used?
//  private final Set<SSHSftpClient> sftpClientSet = ConcurrentHashMap.newKeySet();
//  private final Set<SSHExecChannel> execChannelSet = ConcurrentHashMap.newKeySet();
//  private final Set<SSHScpClient> scpClientSet = ConcurrentHashMap.newKeySet();
  private final AtomicLong reservationCount = new AtomicLong();
  private boolean stale = false;

  public SSHConnectionHolder(SSHConnection conn) { sshConnection = conn; }

  public long reserve()
  {
    return reservationCount.incrementAndGet();
  }

  /*
   * Decrement the reservation count and close the connection if the count is zero and the connection is stale
   * Note that the call to stop the connection may get called more than once.
   *  This should be OK. If it is a problem we can synchronize this method.
   */
  public long release()
  {
    long cnt = reservationCount.decrementAndGet();
    if (cnt <= 0 && stale) sshConnection.stop();
    return cnt;
  }

//  /**
//   * Stop the connection and invalidate the cache key if connection is not in use
//   * @param cache - LoadingCache containing the key to be invalidated.
//   * @param key - Key to invalidate.
//   * @return true if connection was stopped and key invalided else false
//   */
//  public boolean stopConnectionIfUnused(LoadingCache cache, SSHConnectionCacheKey key)
//  {
//    // TODO
//    if (false)
//    {
//      cache.invalidate(key);
//      sshConnection.stop();
//    }
//    return false;
//  }
//
  /*
   *  Get/Return SFTP client
   */
  public synchronized SSHSftpClient getSftpClient() throws IOException
  {
    SSHSftpClient client;
    // TODO/TBD: Need to make sure sshConnection has a session?
    //           Otherwise getSftpClient will fail with NPE
    //   github issue: https://github.com/tapis-project/tapis-files/issues/39
    //   see SSHConnectionCache
//    ClientSession session =  sshConnection.getSession();
    // TODO/TBD:

    client = sshConnection.getSftpClient();
//    sftpClientSet.add(client);
    return client;
  }
  public synchronized void returnSftpClient(SSHSftpClient client) throws IOException
  {
    client.close();
//    sftpClientSet.remove(client);
  }

  /*
   *  Get/Return Exec Channel
   */
  public synchronized SSHExecChannel getExecChannel()
  {
    SSHExecChannel channel = sshConnection.getExecChannel();
//    execChannelSet.add(channel);
    return channel;
  }
  public synchronized void returnExecChannel(SSHExecChannel channel) { /*execChannelSet.remove(channel);*/ }

  /*
   *  Get/Return SCP client
   */
  public synchronized SSHScpClient getScpClient() throws IOException
  {
    SSHScpClient scpClient;
    scpClient = sshConnection.getScpClient();
//    scpClientSet.add(scpClient);
    return scpClient;
  }
  public synchronized void returnScpClient(SSHScpClient client) { /*scpClientSet.remove(client);*/  }

  public synchronized SSHConnection getSshConnection() { return sshConnection; }

//  public synchronized int getChannelCount()
//  {
//    return sftpClientSet.size() + scpClientSet.size() + execChannelSet.size();
//  }

  public boolean isStale() { return stale; }
  public void makeStale() { stale = true; }
}
