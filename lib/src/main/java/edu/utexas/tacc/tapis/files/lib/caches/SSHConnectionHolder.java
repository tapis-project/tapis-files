package edu.utexas.tacc.tapis.files.lib.caches;

import edu.utexas.tacc.tapis.shared.ssh.apache.SSHConnection;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHExecChannel;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHScpClient;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHSftpClient;
import org.apache.sshd.sftp.client.SftpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SSHConnectionHolder {
    private static final Logger log = LoggerFactory.getLogger(SSHConnectionHolder.class);
    private final SSHConnection sshConnection;
    private final Set<SSHSftpClient> sftpClientSet = ConcurrentHashMap.newKeySet();
    private final Set<SSHExecChannel> execChannelSet = ConcurrentHashMap.newKeySet();
    private final Set<SSHScpClient> scpClientSet = ConcurrentHashMap.newKeySet();

    public SSHConnectionHolder(SSHConnection sshConnection) {
        this.sshConnection = sshConnection;
    }

    public synchronized void returnSftpClient(SSHSftpClient client) throws IOException {
        client.close();
        sftpClientSet.remove(client);
        log.info("returnSftpClient: {}", getChannelCount());
    }

    public synchronized SSHSftpClient getSftpClient() throws IOException {
        SSHSftpClient client;
        try {
            client = sshConnection.getSftpClient();
        } catch (IOException ex) {
            throw ex;
        }
        sftpClientSet.add(client);
        log.info("getSftpClient: {}", getChannelCount());
        return client;
    }

    public synchronized SSHExecChannel getExecChannel() {
        log.info("getExecChannel: {}", getChannelCount());
        SSHExecChannel channel = sshConnection.getExecChannel();
        execChannelSet.add(channel);
        return channel;
    }

    public synchronized void returnExecChannel(SSHExecChannel channel) {
        execChannelSet.remove(channel);
        log.info("returnExecChannel: {}", getChannelCount());
    }

    public synchronized SSHScpClient getScpClient() throws IOException {
        SSHScpClient scpClient;
        try {
            scpClient = sshConnection.getScpClient();
        } catch (IOException ex) {
            throw ex;
        }
        scpClientSet.add(scpClient);
        return scpClient;
    }

    public synchronized void returnScpClient(SSHScpClient client) {
        scpClientSet.remove(client);
    }


    public synchronized SSHConnection getSshConnection() {
        return sshConnection;
    }


    public synchronized int getChannelCount() {
        return sftpClientSet.size() + scpClientSet.size() + execChannelSet.size();
    }

}
