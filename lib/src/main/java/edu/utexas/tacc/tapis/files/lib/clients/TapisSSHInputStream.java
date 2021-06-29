package edu.utexas.tacc.tapis.files.lib.clients;

import com.jcraft.jsch.Channel;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHConnection;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;


/**
 *  In order to safely close an InputStream from a command or sftp channel, we need to
 *  also need to return the channel to the session and close that channel.
 */
public class TapisSSHInputStream extends FilterInputStream {


    private SSHConnection connection;
    private Channel channel;

    protected TapisSSHInputStream(InputStream in) {
        super(in);
    }

    @Override
    public int read() throws IOException {
        return super.read();
    }

    public TapisSSHInputStream(InputStream in, SSHConnection connection) {
        super(in);
        this.connection = connection;
    }

    @Override
    public void close() throws IOException {
        super.close();
        this.connection.close();
    }

    @Override
    public byte[] readAllBytes() throws IOException {
        return super.readAllBytes();
    }
}
