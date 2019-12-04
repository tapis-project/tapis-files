package edu.utexas.tacc.tapis.files.lib.kernel;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Vector;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.UIKeyboardInteractive;
import com.jcraft.jsch.UserInfo;
import com.jcraft.jsch.ChannelSftp.LsEntry;

import edu.utexas.tacc.tapis.files.lib.exceptions.FilesKernelException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;

/**
 * @author ajamthe
 * This class is the core of the Files Kernel
 * It uses the JSch API to establish sftp channel and securely transfer file to
 * the destination host 
 */
public class SftpFilesKernel {
	/* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
 
	private static final int CONNECT_TIMEOUT_MILLIS = 15000; // 15 seconds
	private static final String CHANNEL_TYPE = "sftp";
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
  
	private final String host;
	private final int port;
	private final String username;
	private final AuthMethod authMethod;
	private String password;
	private byte[] privateKey;
	private byte[] publicKey;
	private byte[] passPhrase;
	private String identity;
	private Session session;
	private ChannelSftp channelSftp;

	// Local logger.
	private static final Logger _log = LoggerFactory.getLogger(SftpFilesKernel.class);

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
	 * @param user user having access to the remote system
	 * @param port connection port number. If user defined port is greater
	 * than 0 then set it or default to 22.
	 * @param publicKey public Key of the user
	 * @param privateKey private key of the user This constructor will get called
	 * if user chooses ssh keys to authenticate to remote host
	 * This will set authMethod to PUBLICKEY_AUTH
	 * Private Key needs to be in SSLeay/traditional format 
	 */
	public SftpFilesKernel(String host, String username, int port, byte[] privateKey, byte[] publicKey,
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
	public SftpFilesKernel(String host, int port, String username, String password) {
		super();
		this.host = host;
		this.port = port > 0 ? port : 22;
		this.username = username;
		this.password = password;
		authMethod = AuthMethod.PASSWORD_AUTH;

	}

	/* ********************************************************************** */
	/* 						Public Methods */
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
		// Open Sftp Channel since session connect is successful
		try {
			_log.debug("Trying to open SSH Channel");
			channelSftp = (ChannelSftp) session.openChannel(CHANNEL_TYPE);
			_log.debug("Open SSH Channel successful");
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

	}

	/**
	 * @param source
	 * @param destination
	 * @return
	 * @throws FilesKernelException
	 */
	public boolean transferFile(@NotNull String source, @NotNull String destination) throws FilesKernelException {
		if (session != null && channelSftp != null) {
		try {
				ProgressMonitor progress = new ProgressMonitor();
				channelSftp.put(source, destination, progress);
				return true;

		} catch (SftpException e) {
			String Msg = "FK_FILE_TRANSFER_ERROR in method "+ this.getClass().getName() +" for user:  " 
		            + username + "on destination host: "
					+ host + " : " + e.toString();
						_log.error(Msg,e);
		_log.error(Msg,e);
			throw new FilesKernelException(Msg,e);
		}
	   }
			
	return false;
		
	}
	
	/**
	 * Returns the files listing output on a remotePath
	 * @param remotePath
	 * @return list of FileInfo
	 * @throws FilesKernelException
	 */
	public List<FileInfo> ls(@NotNull String remotePath) throws FilesKernelException{
		
		List<FileInfo> filesList = new ArrayList<FileInfo>();
		
		if (session != null && channelSftp != null) {
			try {
				_log.debug("SFTPFilesKernel ls remotePath: " + remotePath);
				Vector<?> filelist = channelSftp.ls(remotePath);
	            for(int i=0; i<filelist.size();i++){
	                LsEntry entry = (LsEntry)filelist.get(i);	
	                SftpATTRS attrs = entry.getAttrs();
	                
	                FileInfo fileInfo = new FileInfo();
	                if(entry.getFilename().equals(".") || entry.getFilename().equals("..")){
	                	continue;
	                }
	                
	                fileInfo.setName(entry.getFilename());
	                
	                DateTimeFormatter dateTimeformatter = DateTimeFormatter.ofPattern( "EEE MMM d HH:mm:ss zzz uuuu" , Locale.US);
	                ZonedDateTime lastModified = ZonedDateTime.parse(attrs.getMtimeString(), dateTimeformatter);
	                fileInfo.setLastModified(lastModified.toInstant());
	                
	                fileInfo.setSize(attrs.getSize());
	                
	                if(attrs.isReg()) {	
	                	Path fullPath = Paths.get(remotePath, entry.getFilename());
	                	fileInfo.setPath(fullPath.toString());
	                } else {
	                	fileInfo.setPath(remotePath);
	                }
	                
	                filesList.add(fileInfo);
		                        
	            }
	        
	            return filesList;

			} catch (SftpException e) {
				String Msg = "FK_FILE_LISTING_ERROR in method "+ this.getClass().getName() +" for user:  " 
			            + username + " on destination host: "
						+ host + " path :"+ remotePath + " : " + e.toString();
							_log.error(Msg,e);
			_log.error(Msg,e);
				throw new FilesKernelException(Msg,e);
			 } 
			
		   }
				
		return filesList;
	}

	/**
	 * Disconnect session
	 */
	public void closeSession() {

		if (channelSftp != null && channelSftp.isConnected())
			// Close channel
			channelSftp.disconnect();
		// Close session
		if (session != null && session.isConnected())
			session.disconnect();

	}
}
