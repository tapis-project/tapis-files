package edu.utexas.tacc.tapis.files.lib.kernel;

import java.io.IOException;

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

import edu.utexas.tacc.tapis.files.lib.exceptions.FilesKernelException;


public class SSHConnection {
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */

    private static final int CONNECT_TIMEOUT_MILLIS = 15000; // 15 seconds
    //private static final String CHANNEL_TYPE = "sftp";
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
    
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(SSHConnection.class);

    /* ********************************************************************** */
    /* Constructors */
    /* ********************************************************************** */
    /*
     * ----------------------------------------------------------------------------
     */
    /* constructor: */
    /*
     * ----------------------------------------------------------------------------
     */
    //  TODO: Fix //
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
     *
     * @throws JSchException
     * @throws IOException
     */
    public void initSession() throws FilesKernelException {

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
            String Msg = "FK_GET_SESSION_ERROR in method " + this.getClass().getName() +" for user:  "
                    + username + "on destination host: "
                    + host + " : " + e.toString();
            _log.error(Msg,e);
            throw new FilesKernelException(Msg,e);
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
                String Msg = "FK_ADD_KEY_ERROR in method "+ this.getClass().getName() +" for user:  "
                        + username + "on destination host: "
                        + host + " : " + e.toString();
                _log.error(Msg, e);
                throw new FilesKernelException(Msg, e);
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
            String Msg = "FK_CONEECT_SESSION_ERROR in method "+ this.getClass().getName() +" for user:  "
                    + username + "on destination host: "
                    + host + " : " + e.toString();
            _log.error(Msg,e);
            throw new FilesKernelException(Msg,e);
        }
        

    }
    
    public Channel openAndConnectChannelSFTP() throws FilesKernelException {
        String CHANNEL_TYPE = "sftp";
        ChannelSftp channelSftp;
        
        try {
            _log.debug("Trying to open sftp  Channel");
            channelSftp = (ChannelSftp) session.openChannel(CHANNEL_TYPE);
            _log.debug("Open STP Channel successful");
        } catch (JSchException e) {
            String Msg = "FK_OPEN_SFTP_CHANNEL_ERROR in method "+ this.getClass().getName() +" for user:  "
                    + username + "on destination host: "
                    + host + " : " + e.toString();
            _log.error(Msg,e);
            throw new FilesKernelException(Msg,e);
        }
        try {
            _log.debug("Trying to connect the sftp Channel");
            channelSftp.connect();
            _log.debug("Channel open OK");
        } catch (JSchException e) {
            String Msg = "FK_CONNECT_SFTP_CHANNEL_ERROR in method "+ this.getClass().getName() +" for user:  "
                    + username + "on destination host: "
                    + host + " : " + e.toString();
            _log.error(Msg,e);
            throw new FilesKernelException(Msg,e);
        }

        return channelSftp;
    }
    
    public Channel openAndConnectChannelEXEC() throws FilesKernelException {
        String CHANNEL_TYPE = "exec";
        ChannelExec channelExec;
        try {
            _log.debug("Trying to open sftp  Channel");
             channelExec= (ChannelExec)session.openChannel(CHANNEL_TYPE);
             
             
            _log.debug("Open STP Channel successful");
        } catch (JSchException e) {
            String Msg = "FK_OPEN_SFTP_CHANNEL_ERROR in method "+ this.getClass().getName() +" for user:  "
                    + username + "on destination host: "
                    + host + " : " + e.toString();
            _log.error(Msg,e);
            throw new FilesKernelException(Msg,e);
        }
        try {
            _log.debug("Trying to connect the sftp Channel");
            channelExec.connect();
            _log.debug("Channel open OK");
        } catch (JSchException e) {
            String Msg = "FK_CONNECT_SFTP_CHANNEL_ERROR in method "+ this.getClass().getName() +" for user:  "
                    + username + "on destination host: "
                    + host + " : " + e.toString();
            _log.error(Msg,e);
            throw new FilesKernelException(Msg,e);
        }

        return channelExec;
    }
    public void closeSession() {
       
        // Close session
        if (session != null && session.isConnected())
            session.disconnect();
    }
    public Session getSession(){
        return this.session;
    }


}
