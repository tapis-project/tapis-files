package edu.utexas.tacc.tapis.files.lib.kernel;

import com.jcraft.jsch.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


/**
 * A SSHConnection is basically a wrapper around a Jsch session. Channels that are in use
 * are stored in a concurrent Set data structure. If that set is empty, the session can be safely
 * closed. The openChannel and returnChannel methods are synchronized for thread safety. These objects are
 * stored in the Cache
 */
public class SSHConnection {

    private static final int CONNECT_TIMEOUT_MILLIS = 15000; // 15 seconds

    // A  set that will be used to store the channels that are open
    // on the SSH session.
    private final Set<Channel> channels = new HashSet<>();


    // Indicates what to do if the server's host key changed or the server is
    // unknown. One of yes (refuse connection), ask (ask the user whether to add/change the
    // key) and no (always insert the new key).
    private static final String STRICT_HOSTKEY_CHECKIN_KEY = "StrictHostKeyChecking";
    private static final String STRICT_HOSTKEY_CHECKIN_VALUE = "no";

    private enum AuthMethod {PUBLICKEY_AUTH, PASSWORD_AUTH}
    private static final Logger log = LoggerFactory.getLogger(SSHConnection.class);

    private final String host;
    private final int port;
    private final String username;
    private final AuthMethod authMethod;
    private String password;
    private String privateKey;
    private String publicKey;
    private Session session;

    /**
     * Public/Private key auth
     * @param host Hostname
     * @param username username of user on the remote system
     * @param port Port to connect to, defaults to 22
     * @param publicKey The public key
     * @param privateKey The private key
     * @throws IOException Throws an exception if the session can't connect or a channel could not be opened.
     */
    public SSHConnection(String host, String username, int port, String publicKey, String privateKey)  throws IOException {
        this.host = host;
        this.username = username;
        this.port = port > 0 ? port : 22;
        this.privateKey = privateKey;
        this.publicKey = publicKey;
        authMethod = AuthMethod.PUBLICKEY_AUTH;
        initSession();
    }

    /**
     * Username/password auth
     * @param host
     * @param port
     * @param username
     * @param password
     * @throws IOException
     */
    public SSHConnection(String host, int port, String username, String password) throws IOException {
        this.host = host;
        this.port = port > 0 ? port : 22;
        this.username = username;
        this.password = password;
        authMethod = AuthMethod.PASSWORD_AUTH;
        initSession();
    }

    public synchronized int getChannelCount() {
        return channels.size();
    }

    private void initSession() throws IOException {
        final JSch jsch = new JSch();
        try {
            session = jsch.getSession(username, host, port);
            session.setConfig(STRICT_HOSTKEY_CHECKIN_KEY, STRICT_HOSTKEY_CHECKIN_VALUE);
            session.setConfig("PreferredAuthentications", "password,publickey");
            session.setTimeout(CONNECT_TIMEOUT_MILLIS);

        } catch (JSchException e) {
            String msg = String.format("SSH_CONNECTION_GET_SESSION_ERROR for user %s on host %s", username, host);
            throw new IOException(msg, e);
        }

        if (authMethod == AuthMethod.PUBLICKEY_AUTH) {
            try {
                jsch.addIdentity(host, privateKey.getBytes(), publicKey.getBytes(), (byte[]) null);
            } catch (JSchException e) {
                String msg = String.format("SSH_CONNECTION_ADD_KEY_ERROR for user %s on host %s", username, host);
                throw new IOException(msg, e);
            }
        } else {
            UserInfo ui;
            session.setPassword(password);
            ui = new UserInfoImplementation(username, password);
            session.setUserInfo(ui);
        }

        try {
            session.connect();
        } catch (JSchException e) {
            String msg = String.format("SSH_CONNECT_SESSION_ERROR for user %s on host %s", username, host);
            throw new IOException(msg, e);
        }
    }

    /**
     * Synchronized to allow for thread safety, this method closes a channel and removes it from the
     * set of open channels.
     * @param channel returns and closes a channel from the pool
     */
    public synchronized void returnChannel(Channel channel) {
        channels.remove(channel);
        channel.disconnect();
    }

    /**
     * Create and open a channel on the session, placing it into the concurrent set of channels
     * that are active on the session.
     * @param channelType One of "sftp", "exec" or "shell"
     * @return Channel
     * @throws IOException
     */
    public synchronized Channel createChannel(String channelType) throws IOException {
        Channel channel;
        try {
            if (!session.isConnected()) {
                initSession();
            }
            switch (channelType) {
                case "sftp":
                    channel = (ChannelSftp) session.openChannel(channelType);
                    break;
                case "exec":
                    channel = (ChannelExec) session.openChannel(channelType);
                    break;
                case "shell":
                    channel = session.openChannel(channelType);
                    break;
                default:
                    throw new IOException("Invalid channel type");
            }
            channels.add(channel);
            return channel;

        } catch (JSchException e) {
            String msg = String.format("SSH_OPEN_CHANNEL_ERROR for user %s on host %s", username, host);
            throw new IOException(msg, e);
        }

    }

    public synchronized void closeSession() {
        if (session != null && session.isConnected() && getChannelCount() == 0) {
            session.disconnect();
        }
    }

    public Session getSession() {
        return this.session;
    }


}
