package edu.utexas.tacc.tapis.files.lib.kernel;

import com.jcraft.jsch.Channel;

public class TapisSSHChannel implements AutoCloseable {

    private final Channel channel;

    public TapisSSHChannel(Channel channel) {
        this.channel = channel;
    }

    @Override
    public void close() {
        channel.disconnect();
    }
}
