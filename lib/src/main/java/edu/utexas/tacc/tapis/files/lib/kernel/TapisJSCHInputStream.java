package edu.utexas.tacc.tapis.files.lib.kernel;

import com.jcraft.jsch.Channel;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class TapisJSCHInputStream extends FilterInputStream {


    private SSHConnection connection;
    private Channel channel;

    protected TapisJSCHInputStream(InputStream in) {
        super(in);
    }

    @Override
    public int read() throws IOException {
        return super.read();
    }

    public TapisJSCHInputStream(InputStream in, SSHConnection connection, Channel channel) {
        super(in);
        this.connection = connection;
        this.channel = channel;
    }

    @Override
    public void close() throws IOException {
        super.close();
        this.connection.returnChannel(channel);
    }

    @Override
    public byte[] readAllBytes() throws IOException {
        return super.readAllBytes();
    }
}
