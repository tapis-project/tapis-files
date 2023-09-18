package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.shared.ssh.SshSessionPool;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHSftpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;


/**
 *  In order to safely close an InputStream from a command or sftp channel, we need to
 *  also need to return the channel to the session and close that channel.
 */
public class TapisSSHInputStream extends FilterInputStream {
    private SshSessionPool.PooledSshSession<SSHSftpClient> sftpClient;
    private static final Logger log = LoggerFactory.getLogger(TapisSSHInputStream.class);

    protected TapisSSHInputStream(InputStream in) {
        super(in);
    }

    @Override
    public int read() throws IOException {
        return super.read();
    }

    public TapisSSHInputStream(InputStream in, SshSessionPool.PooledSshSession<SSHSftpClient> client) {
        super(in);
        this.sftpClient = client;
    }

    @Override
    public void close() throws IOException {
        super.close();
        sftpClient.close();
    }

    @Override
    public byte[] readAllBytes() throws IOException {
        return super.readAllBytes();
    }
}
