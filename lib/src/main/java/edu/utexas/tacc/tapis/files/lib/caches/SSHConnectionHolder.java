package edu.utexas.tacc.tapis.files.lib.caches;

import edu.utexas.tacc.tapis.shared.ssh.apache.SSHConnection;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHExecChannel;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHScpClient;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHSftpClient;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.common.channel.Channel;
import org.apache.sshd.common.channel.ChannelListener;
import org.apache.sshd.common.session.Session;
import org.apache.sshd.common.session.SessionListener;
import org.apache.sshd.sftp.client.SftpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class SSHConnectionHolder {
    private static final Logger log = LoggerFactory.getLogger(SSHConnectionHolder.class);
    private final SSHConnection sshConnection;
    private final AtomicInteger counter = new AtomicInteger();

    public SSHConnectionHolder(SSHConnection sshConnection) {
        this.sshConnection = sshConnection;
    }

    public synchronized void returnSftpClient(SSHSftpClient client) throws IOException {
        client.close();
        counter.decrementAndGet();
        log.info("returnSftpClient: {}", counter);
    }

    public synchronized SSHSftpClient getSftpClient() throws IOException {
        try {
            counter.incrementAndGet();
            log.info("getSftpClient: {}", counter);
            return sshConnection.getSftpClient();
        } catch (IOException ex) {
            counter.decrementAndGet();
            log.info("getSftpClient: {}", counter);
            throw ex;
        }
    }

    public synchronized SSHExecChannel getExecChannel() {
        counter.incrementAndGet();
        log.info("getExecChannel: {}", counter);
        return sshConnection.getExecChannel();
    }

    public synchronized void returnExecChannel(SSHExecChannel channel) {
        counter.decrementAndGet();
        log.info("returnExecChannel: {}", counter);
    }

    public synchronized SSHScpClient getScpClient() throws IOException {
        try {
            counter.incrementAndGet();
            return sshConnection.getScpClient();
        } catch (IOException ex) {
            counter.decrementAndGet();
            throw ex;
        }
    }

    public synchronized void returnScpClient() {
        counter.decrementAndGet();
    }


    public synchronized SSHConnection getSshConnection() {
        return sshConnection;
    }


    public synchronized int getChannelCount() {
        return counter.get();
    }

}
