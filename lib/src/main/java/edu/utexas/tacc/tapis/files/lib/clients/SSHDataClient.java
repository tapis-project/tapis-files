package edu.utexas.tacc.tapis.files.lib.clients;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;

import edu.utexas.tacc.tapis.files.lib.exceptions.FilesKernelException;
import edu.utexas.tacc.tapis.files.lib.kernel.SSHConnection;
import edu.utexas.tacc.tapis.files.lib.kernel.SftpFilesKernel;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;

import javax.inject.Named;
import javax.validation.constraints.NotNull;

public class SSHDataClient implements IRemoteDataClient {
    
    private Logger log = LoggerFactory.getLogger(SSHDataClient.class);
	String host;
	int port;
	String username;
	String password;
	String path ;
	TSystem.DefaultAccessMethodEnum accessMethod;
	String remotePath;
	SSHConnection sshConnection;
	String rootDir;
	String systemId;
	ChannelSftp channelSftp;
	
	public SSHDataClient(TSystem system) {
	    host = system.getHost();
		port = system.getPort();
		username = system.getEffectiveUserId();
		password = system.getAccessCredential().getPassword();
		remotePath = system.getBucketName();
		accessMethod = system.getDefaultAccessMethod();
		rootDir = system.getRootDir();
		systemId = system.getName();
		
		
	}
	
	
	/*@Override
	public List<FileInfo> ls(String remotePath) throws IOException {
		List<FileInfo> filesListing = new ArrayList<>();
		Path remoteAbsolutePath = null;
		try {
			 remoteAbsolutePath = Paths.get(rootDir,remotePath);
			 filesListing = ls(remoteAbsolutePath.toString());
		} catch (Exception e) {
		    String msg = "SSH_LISTING_ERROR for system " + systemId + " path: " + remoteAbsolutePath 
		                  + ": " + e.getMessage();
			log.error(msg, e);
			throw new IOException("File Listing Failed" + msg);
		} 
		return filesListing;
	}*/

	
	 /**
     * Returns the files listing output on a remotePath
     * @param remotePath
     * @return list of FileInfo
	 * @throws IOException 
     * @throws FilesKernelException
     */
	@Override
    public List<FileInfo> ls(@NotNull String remotePath) throws IOException{

        List<FileInfo> filesList = new ArrayList<FileInfo>();
        List<?> filelist = null;

        if (sshConnection.getSession() != null) {
            try {
                channelSftp = (ChannelSftp) sshConnection.openAndConnectChannelSFTP();
             } catch (FilesKernelException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            if( channelSftp != null) {
                try {
                    log.debug("SSH DataClient Listing ls remotePath: " + remotePath);
                    filelist = channelSftp.ls(remotePath);
                } catch (SftpException e) {
                    if (e.getMessage().toLowerCase().contains("no such file")) {
                        String msg = "FK_FILE_NOT_FOUND" + " for user: "
                                + username + ", on destination host: "
                                + host + ", path: "+ remotePath + ": " + e.getMessage();
                        log.error(msg, e);
                        throw new FileNotFoundException(msg);
                    } else {
                    String msg = "FK_FILE_LISTING_ERROR in method "+ this.getClass().getName() +" for user: "
                            + username + ", on destination host: "
                            + host + ", path :"+ remotePath + " :  " + e.getMessage();
                        log.error(msg,e);
                    throw new IOException(msg,e);
                    }
                }finally {
                    if (channelSftp != null && channelSftp.isConnected())
                        // Close channel
                        channelSftp.disconnect();
                }
                
                // For each entry in the fileList recieved, get the fileInfo object
                for(int i=0; i<filelist.size();i++){
                    
                    LsEntry entry = (LsEntry)filelist.get(i);
                    
                    // Get the file attributes
                    SftpATTRS attrs = entry.getAttrs();
    
                    FileInfo fileInfo = new FileInfo();
                    
                    // Ignore filename . and ..
                    if(entry.getFilename().equals(".") || entry.getFilename().equals("..")){
                        continue;
                    }
    
                    fileInfo.setName(entry.getFilename());
    
                    // Obtain the modified time for the file from the attribute
                    // and set lastModified field
                    DateTimeFormatter dateTimeformatter = DateTimeFormatter.ofPattern( "EEE MMM d HH:mm:ss zzz uuuu" , Locale.US);
                    ZonedDateTime lastModified = ZonedDateTime.parse(attrs.getMtimeString(), dateTimeformatter);
                    fileInfo.setLastModified(lastModified.toInstant());
    
                    fileInfo.setSize(attrs.getSize());
                    
                    //Check if the entry is a directory or file
                    if(attrs.isReg()) {
                        Path fullPath = Paths.get(remotePath, entry.getFilename());
                        fileInfo.setPath(fullPath.toString());
                    } else {
                        fileInfo.setPath(remotePath);
                    }
    
                    filesList.add(fileInfo);
    
                }

            } else {
                String msg = "FK_FILE_CHANNEL_NULL_ERROR in method "+ this.getClass().getName() +" for user:  "
                        + username + " on destination host: "
                        + host + " path :"+ remotePath ;
                log.error(msg);
               throw new IOException(msg);
            }
            
        } else {
            String msg = "FK_FILE_SESSION_NULL_ERROR in method "+ this.getClass().getName() +" for user:  "
                    + username + " on destination host: "
                    + host + " path :"+ remotePath ;
            log.error(msg);
           throw new IOException(msg);
        }
        return filesList;
    }
    
	/*@Override
	public void mkdir(String remotePath) throws IOException {
	    	        
            Path remoteAbsolutePath = Paths.get(rootDir,remotePath);
            log.debug("SSHDataClient: mkdir call abs path: " + remoteAbsolutePath);
        try { 
            String mkdirStatus = sftp.mkdir(remoteAbsolutePath.toString());
            log.debug("File mkdir status from remote execution: " + mkdirStatus);
            
       } catch (FilesKernelException e) {
           String msg = "SSH_MKDIR_ERROR for system " + systemId + " remote path: " + remoteAbsolutePath 
                   +  " : " + e.getMessage();
           log.error(msg, e);
          throw new IOException("mkdir Failed: " + msg);
       }

	}*/
	 /**
     * Creates a directory on a remotePath and returns the mkdir status 
     * @param remotePath
     * @return mkdir status
     * @throws FilesKernelException
     */
    public void mkdir(@NotNull String remotePath) throws IOException{

        if (sshConnection.getSession() != null) {
            try {
                channelSftp = (ChannelSftp) sshConnection.openAndConnectChannelSFTP();
             } catch (FilesKernelException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            if( channelSftp != null) {
                try {
                    log.debug("SFTPFilesKernel mkdir remotePath: " + remotePath);
                    
                    channelSftp.mkdir(remotePath);  // do we need to set some permission on the directory??
                } catch (SftpException e) {
                    String Msg = "FK_FILE_MKDIR_ERROR in method "+ this.getClass().getName() +" for user:  "
                            + username + " on destination host: "
                            + host + " path :"+ remotePath + " : " + e.toString();
                    log.error(Msg,e);
                   
                    throw new IOException(Msg,e);
                }
                
                String mkdirStatus = "";
                //check if the directory creation was successful
                // if we are able to list the directory, the directory is created
                try {
                    if(channelSftp.ls(remotePath).size() > 0) {
                        mkdirStatus = "Directory" + remotePath + " was created";
                        
                    } else {
                        mkdirStatus = "Directory" + remotePath + " was not created";
                    }
                } catch (SftpException e) {
                    String Msg = "FK_FILE_MKDIR_LISTING_ERROR in method "+ this.getClass().getName() +" for user:  "
                            + username + " on destination host: "
                            + host + " path :"+ remotePath + " : " + e.toString();
                    log.error(Msg,e);
                   throw new IOException(Msg,e);
                }finally {
                    if (channelSftp != null && channelSftp.isConnected())
                        // Close channel
                        channelSftp.disconnect();
                }
                
           
    
            } else {
                String msg = "FK_FILE_CHANNEL_NULL_ERROR in method "+ this.getClass().getName() +" for user:  "
                        + username + " on destination host: "
                        + host + " path :"+ remotePath ;
                log.error(msg);
               throw new IOException(msg);
            }
        
    } else {
        String msg = "FK_FILE_SESSION_NULL_ERROR in method "+ this.getClass().getName() +" for user:  "
                + username + " on destination host: "
                + host + " path :"+ remotePath ;
        log.error(msg);
       throw new IOException(msg);
    }
       
    }
    

	@Override
	public void insert(String remotePath, InputStream fileStream) throws IOException {

	}

	/*@Override
	public void move(String oldPath, String newName) throws IOException {
	    Path remoteAbsoluteOldPath = Paths.get(rootDir,oldPath);
	    Path remoteAbsoluteNewPath = Paths.get(remoteAbsoluteOldPath.getParent().toString(), newName);
        log.debug("SSHDataClient: move call abs old path: " + remoteAbsoluteOldPath);
        log.debug("SSHDataClient: move call abs new path: " + remoteAbsoluteNewPath);
    try { 
        String renameStatus = sftp.rename( remoteAbsoluteOldPath.toString(), remoteAbsoluteNewPath.toString());
        log.debug("File rename status from remote execution: " + renameStatus);
        
   } catch (FilesKernelException e) {
       String msg = "SSH_RENAME_ERROR for system " + systemId + " old path: " + remoteAbsoluteOldPath 
               + " to newPath: "+ remoteAbsoluteNewPath + " : " + e.getMessage();
       log.debug(msg, e);
      throw new IOException("Rename Failed: " + msg);
   }
}*/
	/**
     * Rename/move file/directory to newPath
     * Returns the mv/rename status
     * @param oldPath
     * @param newPath
     * @return mv/rename status
	 * @throws IOException 
     * @throws FilesKernelException
     */
    public void move(@NotNull String oldPath, @NotNull String newPath) throws IOException{

        if (sshConnection.getSession()!= null) {
            try {
                channelSftp = (ChannelSftp) sshConnection.openAndConnectChannelSFTP();
             } catch (FilesKernelException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            if( channelSftp != null) {
           
            try {
                log.debug("SSH DataClient move oldPath: " + oldPath);
                log.debug("SSH DataClient move newPath: " + newPath);
                
                channelSftp.rename(oldPath, newPath);
               
            } catch (SftpException e) {
                if (e.getMessage().toLowerCase().contains("no such file")) {
                    String msg = "FK_FILE_NOT_FOUND" + " for user: "
                            + username + ", on destination host: "
                            + host + ", path: "+ oldPath + ": " + e.getMessage();
                    log.error(msg, e);
                    throw new FileNotFoundException(msg);
                } else {
                    String Msg = "FK_FILE_RENAME_ERROR in method "+ this.getClass().getName() +" for user:  "
                            + username + " on destination host: "
                            + host + " old path :"+ oldPath + " : " + " newPath: "+ newPath + ":" + e.getMessage();
                    log.error(Msg,e);
                   
                    throw new IOException(Msg,e);
            
                    }
                }
            
            String renameStatus = "";
            try {
                
                if(channelSftp.stat(newPath) != null) {
                    renameStatus = "Rename" + oldPath + " to " + " newPath"+  "was successful";
                    
                   
                } else {
                    renameStatus = "Rename" + oldPath + " to " + " newPath"+  "was not successful"; 
                    
                }
            } catch (SftpException e) {
                String Msg = "FK_FILE_RENAME_STAT_ERROR in method "+ this.getClass().getName() +" for user:  "
                        + username + " on destination host: "
                        + host + " old path :"+ oldPath + " : " + " newPath: "+ newPath + ":" + e.getMessage();
                log.error(Msg,e);
               throw new IOException(Msg,e);
            }finally {
                if (channelSftp != null && channelSftp.isConnected())
                    // Close channel
                    channelSftp.disconnect();
            }
            
           

        } else {
            String msg = "FK_FILE_SESSION_OR_CHANNEL_NULL_ERROR in method "+ this.getClass().getName() +" for user:  "
                    + username + " on destination host: "
                    + host + " old path :"+ oldPath + " newPath: " + newPath ;
            log.error(msg);
           throw new IOException(msg);
        } }else {
            
            String msg = "FK_FILE_SESSION_NULL_ERROR in method "+ this.getClass().getName() +" for user:  "
                    + username + " on destination host: "
                    + host + " path :"+ remotePath ;
            log.error(msg);
           throw new IOException(msg);
            }
        
        }

        
    

	@Override
	public void copy(String currentPath, String newPath) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void delete(String path) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public InputStream getStream(String path) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void download(String path) {
		// TODO Auto-generated method stub

	}

	@Override
	public void connect() throws IOException {
		
		switch(accessMethod.getValue()) {
		case "PASSWORD":
			System.out.println("host: "+ host+ " port: "+ port+ " username: "+ username + " password: "+ password);
			sshConnection = new SSHConnection(host, port, username, password);
			try {
			    sshConnection.initSession();
			    
			} catch (FilesKernelException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		case "SSH_KEYS":
			//
		default:
			//dosomething
		}

	}

	@Override
	public void disconnect() {
		sshConnection.closeSession();

	}

	@Override
	public InputStream getBytesByRange(String path, long startByte, long endByte) throws IOException{
		return null;
	}

	@Override
	public void putBytesByRange(String path, InputStream byteStream, long startByte, long endByte) throws IOException {

	}

	@Override
	public void append(String path, InputStream byteStream) throws IOException {

	}

}
