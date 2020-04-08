package edu.utexas.tacc.tapis.files.lib.kernel;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UIKeyboardInteractive;
import com.jcraft.jsch.UserInfo;



public class SSHConnection {
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */

    private static final int CONNECT_TIMEOUT_MILLIS = 15000; // 15 seconds
 

    //
    // Indicates what to do if the server's host key changed or the server is
    // unknown.
    // One of yes (refuse connection), ask (ask the user whether to add/change the
    // key) and no
    // (always insert the new key).
    private static final String STRICT_HOSTKEY_CHECKIN_KEY = "StrictHostKeyChecking";;
    private static final String STRICT_HOSTKEY_CHECKIN_VALUE = "no";

    /* **************************************************************************** */
    /*                                   Enum                                    */
    /* **************************************************************************** */
    private enum AuthMethod {PUBLICKEY_AUTH, PASSWORD_AUTH};

    /* **************************************************************************** */
    /*                                    Fields                                    */
    /* **************************************************************************** */

    protected final String host;
    protected final int port;
    protected final String username;
    protected final AuthMethod authMethod;
    protected String password;
    protected byte[] privateKey;
    protected byte[] publicKey;
    protected byte[] passPhrase;
    protected String identity;
    protected Session session;
    //protected Channel channel;

    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(SSHConnection.class);

    /* ********************************************************************** */
    /* Constructors */
    /* ********************************************************************** */
   
    /**
     * @param host Destination host name for files/dir copy
     * @param username user having access to the remote system
     * @param port connection port number. If user defined port is greater
     * than 0 then set it or default to 22.
     * @param publicKey public Key of the user
     * @param privateKey private key of the user This constructor will get called
     * if user chooses ssh keys to authenticate to remote host
     * This will set authMethod to PUBLICKEY_AUTH
     * Private Key needs to be in SSLeay/traditional format
     */
    public SSHConnection(String host, String username, int port, byte[] privateKey, byte[] publicKey,
            byte[] passphrase, String identity) {
        super();
        this.host = host;
        this.username = username;
        this.port = port > 0 ? port : 22;
        this.privateKey = privateKey;
        this.publicKey = publicKey;
        this.passPhrase = passPhrase;
        this.identity = identity;
        authMethod = AuthMethod.PUBLICKEY_AUTH;
    }

    /*
     * ----------------------------------------------------------------------------
     */
    /* constructor: */
    /*
     * ----------------------------------------------------------------------------
     */

    /**
     * @param host Destination host name for files/dir copy
     * @param port connection port number. If user defined port is greater
     *                 than 0 then set it or default to 22.
     * @param username user having access to the remote system
     * @param password password This constructor will get called if user chooses
     *                 password to authenticate to remote host This will set the
     *                 authMethod to "passwordAuth";
     */
    public SSHConnection(String host, int port, String username, String password) {
        super();
        this.host = host;
        this.port = port > 0 ? port : 22;
        this.username = username;
        this.password = password;
        authMethod = AuthMethod.PASSWORD_AUTH;

    }

    /* ********************************************************************** */
    /*                      Public Methods */
    /* ********************************************************************** */

    /**
     * Initializes session information and creates a SSH connection
     * @throws JSchException
     * @throws IOException
     */
    public void initSession() throws IOException {

        // Create a JSch object
        final JSch jsch = new JSch();

        // Instantiates the Session object with username and host.
        // The TCP port defined by user will be used in making the connection.
        // Port 22 is the default port.
        // TCP connection must not be established until Session#connect()
        // returns instance of session class
        try {
            session = jsch.getSession(username, host, port);
            // Once getSession is successful set the configuration
            // STRICT_HOSTKEY_CHECKIN_KEY is set to "no"
            // Connection time out is set to 15 seconds
            session.setConfig(STRICT_HOSTKEY_CHECKIN_KEY, STRICT_HOSTKEY_CHECKIN_VALUE);
            session.setConfig("PreferredAuthentications", "password,publickey,keyboard-interactive");
            session.setTimeout(CONNECT_TIMEOUT_MILLIS);

        } catch (JSchException e) {
            String Msg = "SSH_CONNECTION_GET_SESSION_ERROR in method " + this.getClass().getName() +" for user:  "
                    + username + "on destination host: "
                    + host + " : " + e.toString();
            _log.error(Msg,e);
            throw new IOException(Msg,e);
        }

        _log.debug("Try to connect to the host " + host + ":" + port + " with user " + username);

        UserInfo ui = null;
        UIKeyboardInteractive interactive = null;

        if (authMethod == AuthMethod.PUBLICKEY_AUTH) {
            // Adds an identity to be used for public-key authentication
            try {
                jsch.addIdentity(identity, privateKey, publicKey, passPhrase);
                ui = new UserInfoImplementation(username, privateKey);
                _log.debug("identity for public-key authentication successfully added");
            } catch (JSchException e) {
                String Msg = "SSH_CONNECTION_ADD_KEY_ERROR in method "+ this.getClass().getName() +" for user:  "
                        + username + "on destination host: "
                        + host + " : " + e.toString();
                _log.error(Msg, e);
                throw new IOException(Msg, e);
            }

        }
        else {
            // Adds password to be used for password based authentication
            session.setPassword(password);
            // Create a object of Userinfo Implementation class to get the user info
            ui = new UserInfoImplementation(username, password);
            session.setUserInfo(ui);
        }

        try {
            session.connect();
            _log.info("Connection established");
        } catch (JSchException e) {
            String Msg = "SSH_CONNECTION_CONEECT_SESSION_ERROR in method "+ this.getClass().getName() +" for user:  "
                    + username + "on destination host: "
                    + host + " : " + e.toString();
            _log.error(Msg,e);
            throw new IOException(Msg,e);
        }


    }
    
   /**
    * Opens a channel for the SSH connection
    * @param channelType
    * @return channel
    * @throws IOException
    */
    public Channel openChannel(String channelType) throws IOException {
        Channel channel = null;
        try {
            switch(channelType) {
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
                _log.debug("SSH_UNSUPPORTED_CHANNEL");
            }
        } catch(JSchException e) {
            
            String Msg = "SSH_OPEN_"+ channelType.toUpperCase() + "_CHANNEL_ERROR in method "+ this.getClass().getName() +" for user:  "
                    + username + "on destination host: "
                    + host + " port: " + port + " : " + e.toString();
            _log.error(Msg,e);
            throw new IOException(Msg,e);
        }
        return channel; 
        
    }
    
    /**
     * Connects to the channel
     * @param channel
     * @throws IOException
     */
    public void connectChannel(Channel channel) throws IOException {
        
        try {
            _log.debug("Trying to connect to the channel");
            channel.connect();
            _log.debug("Channel connect ok");
        } catch (JSchException e) {
            String Msg = "SSH_CONNECTION_CHANNEL_ERROR in method "+ this.getClass().getName() +" for user:  "
                    + username + "on destination host: "
                    + host + " port: " + port + " : " + e.toString();
            _log.error(Msg,e);
            throw new IOException(Msg,e);
        }
     }
    
    /**
     * Closes the channel
     * @param channel
     */
    public void closeChannel(Channel channel) {
        _log.debug("Trying to close  the channel");
        
        if (channel != null && channel.isConnected())
            // Close channel
            channel.disconnect();
            
    }

    /**
     * Sets the command to be executed in the SSH exec channel    
     * @param channel
     * @param command
     * @param in
     * @param out
     * @param err
     * @return Channel
     * @throws IOException
     */
    public Channel setCommandForExecChannel(Channel channel, String command, InputStream in, OutputStream out, OutputStream err ) throws IOException {
        
        ((ChannelExec)channel).setCommand(command);
        channel.setInputStream(in);
        channel.setOutputStream(out);
        ((ChannelExec)channel).setErrStream(err);
               
        return channel;
        
    }

   /**
    * Closes the session  
    */
    public void closeSession() {
        if (session != null && session.isConnected())
            session.disconnect();
    }

    /**
     * Gets the SSH Session
     * @return
     */
    public Session getSession(){
        return this.session;
    }


}
