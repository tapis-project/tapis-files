package edu.utexas.tacc.tapis.files.lib.kernel; 

import java.io.IOException;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.UIKeyboardInteractive;
import com.jcraft.jsch.UserInfo;

import edu.utexas.tacc.tapis.files.lib.exceptions.FilesKernelException;



/**
 * @author ajamthe
 *
 */
public class SftpFilesKernel {
	/*
	* ****************************************************************************
	*/
	/* Constants */
	/*
	* ****************************************************************************
	*/
	// Local logger.
	private static final Logger _log = LoggerFactory.getLogger(SftpFilesKernel.class);
	
	// Socket timeouts
    private static final int CONNECT_TIMEOUT_MILLIS = 15000;   // 15 seconds
    private static final String CHANNEL_TYPE = "sftp";
 
    // 
    //Indicates what to do if the server's host key changed or the server is unknown. 
    //One of yes (refuse connection), ask (ask the user whether to add/change the key) and no 
    //(always insert the new key).
	private static final String STRICT_HOSTKEY_CHECKIN_KEY = "StrictHostKeyChecking";;
	private static final String STRICT_HOSTKEY_CHECKIN_VALUE = "no";

	/*
	* ****************************************************************************
	*/
	/* Fields */
	/*
	* ****************************************************************************
	*/
	
	private final String host;
	private final int port;
	private final String username;
	private final String authMethod;
	private String password;
	private byte[] privateKey;
	private byte[] publicKey;
	private byte[] passPhrase;
	private String identity;

	private Session session;
	private ChannelSftp channelSftp;
	
	
	/* ********************************************************************** */
	/* Constructors */
	/* ********************************************************************** */
	
	/**
	* @param host // Destination host name for files/dir copy
	* @param user //user having access to the remote system
	* @param port //connection port number. If user defined port is greater than 0 then set it or default to 22.
	* @param publicKey //public Key of the user 
	* @param privateKey //private key of the user 
	* This constructor will get called if user chooses ssh keys to authenticate to remote host
	* This will set authMethod to "publickeyAuth"
	*/
	public SftpFilesKernel(String host, String username, int port, byte[] privateKey, byte[] publicKey, byte[] passphrase, String identity) {
		super();
		this.host = host;
		this.username = username;
		this.port = port > 0 ? port : 22;
		this.privateKey = privateKey;
		this.publicKey = publicKey;
		this.passPhrase = passPhrase;
		this.identity = identity;
		authMethod = "publickeyAuth";			
	}
		
	
	/**
	* @param host // Destination host name for files/dir copy
	* @param port // connection port number. If user defined port is greater than 0 then set it or default to 22.
	* @param username //user having access to the remote system
	* @param password //password
	* This constructor will get called if user chooses password to authenticate to remote host
	* This will set the authMethod to "passwordAuth";
	*/
	public SftpFilesKernel(String host, int port, String username, String password) {
		super();
		this.host = host; 
		this.port = port > 0 ? port : 22;
		this.username = username;
		this.password = password;
		authMethod = "passwordAuth";

	}

	/* ********************************************************************** */
	/* Public Methods */
	/* ********************************************************************** */
		
   /**
    * 
    * @throws JSchException
    * @throws IOException
    */
	public void initSession() throws FilesKernelException {
	   
		//Create a JSch object
		final JSch jsch = new JSch();
		
		//Instantiates the Session object with username and host. 
		//The TCP port defined by user will be used in making the connection. 
		//Port 22 is the default port.
		// TCP connection must not be established until Session#connect() 
		//returns instance of session class
		try {
			session = jsch.getSession(username, host, port);
			//Once getSession is successful set the configuration
			// STRICT_HOSTKEY_CHECKIN_KEY is set to "no"
			// Connection time out is set to 15 seconds 	
			session.setConfig(STRICT_HOSTKEY_CHECKIN_KEY, STRICT_HOSTKEY_CHECKIN_VALUE);
			session.setConfig("PreferredAuthentications",
	                 "password,publickey,keyboard-interactive");
			session.setTimeout(CONNECT_TIMEOUT_MILLIS);
	   
		} catch (JSchException e) {
			 String Msg = "FK_ERROR_GET_SESSION in method initSession()" + e.toString(); 
			 _log.error(Msg);
			 throw new FilesKernelException(Msg);
		}
		
		_log.info("Try to connect to the host " + host +":" + port +" with user "+ username);
		
		UserInfo ui = null;
		UIKeyboardInteractive interactive = null;
		
		if(authMethod.equalsIgnoreCase("publicKeyAuth")) {
		//Adds an identity to be used for public-key authentication
			try {
				jsch.addIdentity(identity,privateKey,publicKey,passPhrase);
	    		 ui = new UserInfoImplementation(username,privateKey);
	    		 _log.info("identity for public-key authentication successfully added");
			} catch (JSchException e) {
				 String Msg = "FK_ERROR_ADD_KEY " + e.toString(); 
				 _log.error(Msg);
				 throw new FilesKernelException(Msg);
			}
			
		}
		

		if(authMethod.equalsIgnoreCase("passwordAuth")){
			//Adds password to be used for password based authentication
    		session.setPassword(password);		
		    // Create a object of Userinfo Implementation class to get the user info
    		ui = new UserInfoImplementation(username, password);
    		session.setUserInfo(ui);
		}
		
    	try {
    		session.connect();
    		_log.info("Connection established");
    	} catch (JSchException e) {
    		String Msg = "FK_ERROR_CONNECT_SESSION in the method initSesion() " + e.toString(); 
    		_log.error(Msg);
    		throw new FilesKernelException(Msg); 			
    		}
		//Open Sftp Channel since session connect is successful
		
		try {
			_log.info("Trying to open SSH Channel");
			channelSftp = (ChannelSftp) session.openChannel(CHANNEL_TYPE);
			_log.info("Open SSH Channel successful");
		} catch (JSchException e) {
			 String Msg = "FK_ERROR_OPEN_SFTPCHANNEL" + e.toString(); 
			 _log.error(Msg);
			 throw new FilesKernelException(Msg);
		}
		try {
			_log.info("Trying to connect the sftp Channel");
			channelSftp.connect();
			_log.info("Channel open OK");
		} catch (JSchException e) {
			String Msg = "FK_ERROR_CONNECT_SFTPCHANNEL" + e.toString(); 
			 _log.error(Msg);
			 throw new FilesKernelException(Msg);
		}
		
   }

	
	

	public String transferFile(@NotNull String source, @NotNull String destination) throws FilesKernelException{
	    String success = "Successfully transfered file";
		if (session.isConnected()) {
	    	try {
				ProgressMonitor progress = new ProgressMonitor();
				channelSftp.put(source, destination, progress);
				return success;

			} catch (SftpException e) {
				String Msg = "FK_ERROR_TRANSFER_FILE " + e.toString(); 
				 _log.error(Msg);
				  throw new FilesKernelException(Msg);
			}
	    }
		return null;
		
		
	}

	//TODO: look at the disconnect declaration to find out what happens if session is not disconnected
	/**
	    * @throws Exception
	    */
	   public void closeSession() {

		   if(channelSftp != null && channelSftp.isConnected())
		   // Close channel
			   channelSftp.disconnect();
		   // Close session
		   if (session != null && session.isConnected()) 
			   session.disconnect();

	   }
}






























