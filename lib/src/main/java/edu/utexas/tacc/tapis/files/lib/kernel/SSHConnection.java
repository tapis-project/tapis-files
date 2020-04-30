package edu.utexas.tacc.tapis.files.lib.kernel;

import com.jcraft.jsch.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;


public class SSHConnection {

    private static final int CONNECT_TIMEOUT_MILLIS = 15000; // 15 seconds

    // A concurrent set that will be used to store the channels that are open
    // on the SSH session.
    private final Set<Channel> channels = ConcurrentHashMap.newKeySet();


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

    public SSHConnection(String host, String username, int port, String publicKey, String privateKey)  throws IOException {
        this.host = host;
        this.username = username;
        this.port = port > 0 ? port : 22;
        this.privateKey = privateKey;
        this.publicKey = publicKey;
        authMethod = AuthMethod.PUBLICKEY_AUTH;
        initSession();
    }

    public SSHConnection(String host, int port, String username, String password) throws IOException {
        this.host = host;
        this.port = port > 0 ? port : 22;
        this.username = username;
        this.password = password;
        authMethod = AuthMethod.PASSWORD_AUTH;
        initSession();
    }

    public int getChannelCount() {
        return channels.size();
    }

    public void initSession() throws IOException {
        final JSch jsch = new JSch();
        try {
            session = jsch.getSession(username, host, port);
            session.setConfig(STRICT_HOSTKEY_CHECKIN_KEY, STRICT_HOSTKEY_CHECKIN_VALUE);
            session.setConfig("PreferredAuthentications", "password,publickey,keyboard-interactive");
            session.setTimeout(CONNECT_TIMEOUT_MILLIS);

        } catch (JSchException e) {
            String msg = "SSH_CONNECTION_GET_SESSION_ERROR in method " + this.getClass().getName() + " for user:  "
                    + username + "on destination host: "
                    + host + " : " + e.toString();
            throw new IOException(msg, e);
        }

        if (authMethod == AuthMethod.PUBLICKEY_AUTH) {
            try {
                jsch.addIdentity(host, privateKey.getBytes(), publicKey.getBytes(), (byte[]) null);
            } catch (JSchException e) {
                String msg = "SSH_CONNECTION_ADD_KEY_ERROR in method " + this.getClass().getName() + " for user:  "
                        + username + "on destination host: "
                        + host + " : " + e.toString();
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
            String msg = "SSH_CONNECTION_CONNECT_SESSION_ERROR in method " + this.getClass().getName() + " for user:  "
                    + username + "on destination host: "
                    + host + " : " + e.toString();
            throw new IOException(msg, e);
        }


    }

    public synchronized void returnChannel(Channel channel) {
        channels.remove(channel);
        channel.disconnect();
    }


    public synchronized Channel openChannel(String channelType) throws IOException {
        Channel channel;
        try {
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

            String msg = "SSH_OPEN_" + channelType.toUpperCase() + "_CHANNEL_ERROR in method " + this.getClass().getName() + " for user:  "
                    + username + "on destination host: "
                    + host + " port: " + port + " : " + e.toString();
            throw new IOException(msg, e);
        }

    }

    public void closeSession() {
        if (session != null && session.isConnected())
            session.disconnect();
    }

    public Session getSession() {
        return this.session;
    }


}
