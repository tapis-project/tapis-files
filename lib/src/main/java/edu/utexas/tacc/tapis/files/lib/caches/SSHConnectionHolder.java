package edu.utexas.tacc.tapis.files.lib.caches;

import edu.utexas.tacc.tapis.shared.ssh.apache.SSHConnection;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHExecChannel;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHScpClient;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHSftpClient;
import org.apache.sshd.client.session.ClientSession;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SSHConnectionHolder
{
  private final SSHConnection sshConnection;
  private final Set<SSHSftpClient> sftpClientSet = ConcurrentHashMap.newKeySet();
  private final Set<SSHExecChannel> execChannelSet = ConcurrentHashMap.newKeySet();
  private final Set<SSHScpClient> scpClientSet = ConcurrentHashMap.newKeySet();

  public SSHConnectionHolder(SSHConnection conn) { sshConnection = conn; }

  /*
   *  Get/Return SFTP client
   */
  public synchronized SSHSftpClient getSftpClient() throws IOException
  {
    SSHSftpClient client;
    // TODO/TBD: Need to make sure sshConnection has a session.
    //           Otherwise getSftpClient will fail with NPE
    //   github issue: https://github.com/tapis-project/tapis-files/issues/39
    //   see SSHConnectionCache
    ClientSession session =  sshConnection.getSession();
    // TODO/TBD:

    client = sshConnection.getSftpClient();
    sftpClientSet.add(client);
    return client;
  }
  public synchronized void returnSftpClient(SSHSftpClient client) throws IOException
  {
    client.close();
    sftpClientSet.remove(client);
  }


  /*
   *  Get/Return Exec Channel
   */
  public synchronized SSHExecChannel getExecChannel()
  {
    SSHExecChannel channel = sshConnection.getExecChannel();
    execChannelSet.add(channel);
    return channel;
  }
  public synchronized void returnExecChannel(SSHExecChannel channel) { execChannelSet.remove(channel); }

  /*
   *  Get/Return SCP client
   */
  public synchronized SSHScpClient getScpClient() throws IOException
  {
    SSHScpClient scpClient;
    scpClient = sshConnection.getScpClient();
    scpClientSet.add(scpClient);
    return scpClient;
  }
  public synchronized void returnScpClient(SSHScpClient client) { scpClientSet.remove(client);  }

  public synchronized SSHConnection getSshConnection() { return sshConnection; }

  public synchronized int getChannelCount()
  {
    return sftpClientSet.size() + scpClientSet.size() + execChannelSet.size();
  }
}
