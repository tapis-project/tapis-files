package edu.utexas.tacc.tapis.files.lib.clients;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import javax.validation.constraints.NotNull;
import javax.ws.rs.NotFoundException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;

import edu.utexas.tacc.tapis.files.lib.kernel.ProgressMonitor;
import edu.utexas.tacc.tapis.files.lib.kernel.SSHConnection;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;

public class SSHDataClient implements IRemoteDataClient {
    
    private Logger log = LoggerFactory.getLogger(SSHDataClient.class);
	String host;
	int port;
	String username;
	String password;
	TSystem.DefaultAccessMethodEnum accessMethod;
	SSHConnection sshConnection;
	String rootDir;
	String systemId;
		
	public SSHDataClient(TSystem system) {
	    host = system.getHost();
		port = system.getPort();
		username = system.getEffectiveUserId();
		password = system.getAccessCredential().getPassword();
		accessMethod = system.getDefaultAccessMethod();
		rootDir = system.getRootDir();
		systemId = system.getName();
	
	}
	
	/**
     * Returns the files listing output on a remotePath
     * @param remotePath
     * @return list of FileInfo
	 * @throws IOException 
     * @throws NotFoundException
     */
	@Override
    public List<FileInfo> ls(@NotNull String remotePath) throws IOException, NotFoundException{

        List<FileInfo> filesList = new ArrayList<FileInfo>();
        List<?> filelist = null;
        Path absolutePath = Paths.get(rootDir,remotePath);
        
        ChannelSftp channelSftp = openAndConnectSFTPChannel(absolutePath.toString());
        
        try {
            log.debug("SSH DataClient Listing ls path: " + absolutePath.toString());
            filelist = channelSftp.ls(absolutePath.toString());
        } catch (SftpException e) {
            if (e.getMessage().toLowerCase().contains("no such file")) {
                String msg = "SSH_DATACLIENT_FILE_NOT_FOUND" + " for user: "
                        + username + ", on host: "
                        + host + ", path: "+ absolutePath.toString() + ": " + e.getMessage();
                log.error(msg, e);
                throw new NotFoundException(msg);
            } else {
                String msg = "SSH_DATACLIENT_FILE_LISTING_ERROR in method "+ this.getClass().getName() +" for user: "
                        + username + ", on host: "
                        + host + ", path :"+ absolutePath.toString() + " :  " + e.getMessage();
                    log.error(msg,e);
                throw new IOException(msg,e);
            }
        } finally {
           sshConnection.closeChannel(channelSftp);
        }
                
        // For each entry in the fileList received, get the fileInfo object
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
            
            //Check if the entry is a directory or a file
            if(attrs.isReg()) {
                Path fullPath = Paths.get(remotePath, entry.getFilename());
                fileInfo.setPath(fullPath.toString());
            } else {
                fileInfo.setPath(remotePath);
            }

            filesList.add(fileInfo);

        }

        return filesList;
    }
    
	
	
	 /**
     * Creates a directory on a remotePath 
     * @return
     * @throws IOException
     * @throws NotFoundException
     */
	@Override
    public void mkdir(@NotNull String remotePath) throws IOException{
	   
	    Path absolutePath = Paths.get(rootDir, remotePath);
	    ChannelSftp channelSftp = openAndConnectSFTPChannel(absolutePath.toString());
        
        try {
            log.debug("SSH DataClient path for mkdir: " + absolutePath.toString());
            
            channelSftp.mkdir(absolutePath.toString());  // do we need to set some permission on the directory??
        } catch (SftpException e) {
            if (e.getMessage().toLowerCase().contains("no such file")) {
                String msg = "SSH_DATACLIENT_FILE_NOT_FOUND" + " for user: "
                        + username + ", on host: "
                        + host + ", path: "+ absolutePath.toString() + ": " + e.getMessage();
                log.error(msg, e);
                throw new NotFoundException(msg);
            } else {
            String Msg = "SSH_DATACLIENT_MKDIR_ERROR in method "+ this.getClass().getName() +" for user:  "
                    + username + " on host: "
                    + host + " path :"+ absolutePath.toString() + " : " + e.toString();
            log.error(Msg,e);
           
            throw new IOException(Msg,e);
            }
        }
                
     
        //check if the directory creation was successful
        // if we are able to list the directory, the directory is created
        try {
            if(channelSftp.ls(remotePath).size() > 0) {
                log.debug( "Directory" +  absolutePath.toString() + " was created");
                
            } else {
                log.debug("Directory" +  absolutePath.toString() + " was not created");
            }
        } catch (SftpException e) {
            String Msg = "SSH_DATACLIENT_FILE_MKDIR_LISTING_ERROR in method "+ this.getClass().getName() +" for user:  "
                    + username + " on host: "
                    + host + " path :"+  absolutePath.toString() + " : " + e.toString();
            log.error(Msg,e);
           throw new IOException(Msg,e);
        } finally {
            sshConnection.closeChannel(channelSftp);
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
     * Rename/move oldPath to newPath
     * @param oldPath
     * @param newPath
     * @return 
	 * @throws IOException 
     * @throws NotFoundException
     */
	@Override
    public void move(@NotNull String oldPath, @NotNull String newPath) throws IOException, NotFoundException{

	    Path absoluteOldPath = Paths.get(rootDir,oldPath);
        Path absoluteNewPath = Paths.get(absoluteOldPath.getParent().toString(), newPath);
        log.debug("SSH DataClient move: oldPath: " + absoluteOldPath.toString());
        log.debug("SSH DataClient move: newPath: " + absoluteNewPath.toString());
	    
        ChannelSftp channelSftp = openAndConnectSFTPChannel(absoluteOldPath.toString(), absoluteNewPath.toString());
	    
           
        try {
            channelSftp.rename(absoluteOldPath.toString(), absoluteNewPath.toString());
        } catch (SftpException e) {
            if (e.getMessage().toLowerCase().contains("no such file")) {
                String msg = "SSH_DATACLIENT_FILE_NOT_FOUND" + " for user: "
                        + username + ", on host: "
                        + host + ", path: "+ absoluteOldPath.toString() + ": " + e.getMessage();
                log.error(msg, e);
                throw new NotFoundException(msg);
            } else {
                String Msg = "SSH_DATACLIENT_RENAME_ERROR in method "+ this.getClass().getName() +" for user:  "
                        + username + " on host: "
                        + host + " old path :"+ absoluteOldPath.toString() + " : " + " newPath: "+ absoluteNewPath.toString() + ":" + e.getMessage();
                log.error(Msg,e);
               
                throw new IOException(Msg,e);
        
                }
            }

            // Check if rename was successful
            try {
                
                if(channelSftp.stat(absoluteNewPath.toString()) != null) {
                    log.debug("Rename" + absoluteOldPath.toString() + " to " + " newPath "+ absoluteNewPath.toString() + " was successful");
                    
                   
                } else {
                    log.debug("Rename" + absoluteOldPath.toString() + " to " + " newPath "+ absoluteNewPath.toString() + " was not successful"); 
                    
                }
            } catch (SftpException e) {
                String Msg = "SSH_DATACLIENT_FILE_RENAME_STAT_ERROR in method "+ this.getClass().getName() +" for user:  "
                        + username + " on host: "
                        + host + " old path :"+ absoluteOldPath.toString() + " : " + " newPath: "+ absoluteNewPath.toString() + ":" + e.getMessage();
                log.error(Msg,e);
               throw new IOException(Msg,e);
            } finally {
                sshConnection.closeChannel(channelSftp);
                
            }
            
        
        }

  
	/**
     * @param currentPath
     * @param newPath
     * @return
     * @throws IOException
     * @throws NotFoundException
     */
	@Override
    public void copy(@NotNull String currentPath, @NotNull String newPath) throws IOException, NotFoundException {
       
        Path absoluteCurrentPath = Paths.get(rootDir,currentPath);
        Path absoluteNewPath = Paths.get(rootDir, newPath);
        log.debug("SSH DataClient copy: currentPath: " + absoluteCurrentPath.toString());
        log.debug("SSH DataClient copy: newPath: " + absoluteNewPath.toString());
        
         ChannelSftp channelSftp = openAndConnectSFTPChannel(absoluteCurrentPath.toString(), absoluteNewPath.toString());
        
            try {
                ProgressMonitor progress = new ProgressMonitor();
                channelSftp.put(absoluteCurrentPath.toString(), absoluteNewPath.toString(), progress);
   
            } catch (SftpException e) {
                if (e.getMessage().toLowerCase().contains("no such file")) {
                    String msg = "SSH_DATACLIENT_FILE_NOT_FOUND" + " for user: "
                            + username + ", on host: "
                            + host + ", path: "+ absoluteCurrentPath.toString() + ": " + e.getMessage();
                    log.error(msg, e);
                    throw new NotFoundException(msg);
                } else {
                String Msg = "SSH_DATACLIENT_FILE_COPY_ERROR in method "+ this.getClass().getName() +" for user:  "
                        + username + ", on host: "
                        + host + ", from current path: " + absoluteCurrentPath.toString() +" to newPath "+ absoluteNewPath.toString()  + " : " + e.toString();
                log.error(Msg,e);
            
                throw new IOException(Msg,e);
                }
            }
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
		case "PASSWORD":{
			log.debug("host: "+ host+ " port: "+ port+ " username: "+ username + " password: "+ password);
			sshConnection = new SSHConnection(host, port, username, password);
			sshConnection.initSession();
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
	
	/* ***************************************************************************************** */
	/*                                     Private Methods                                       */
	/* ***************************************************************************************** */
	/**
	 * Opens and connects to SSH SFTP channel for a SSH connection. 
	 * path parameter is for logging purpose
	 * @param path
	 * @return ChannelSftp
	 * @throws IOException
	 */
	
	private ChannelSftp openAndConnectSFTPChannel(String path) throws IOException {
	    String CHANNEL_TYPE = "sftp";
	    if (sshConnection.getSession() == null) {
            String msg = "SSH_DATACLIENT_SESSION_NULL_ERROR in method "+ this.getClass().getName() +" for user:  "
                    + username + " on destination host: "
                    + host + " path :"+ path ;
            log.error(msg);
           throw new IOException(msg);
        }
           
        ChannelSftp channel = (ChannelSftp) sshConnection.openChannel(CHANNEL_TYPE );
        sshConnection.connectChannel(channel);
             
        if(channel == null) {
            String msg = "SSH_DATACLIENT_CHANNEL_NULL_ERROR in method "+ this.getClass().getName() +" for user:  "
                    + username + " on host: "
                    + host + " path :" + path ;
            log.error(msg);
           throw new IOException(msg);   
        }
        return  channel;
	}
	
	/**
	 * Opens and connects to SSH SFTP channel for a SSH connection.
	 * path parameters are for logging purpose
	 * @param oldPath
	 * @param newPath
	 * @return
	 * @throws IOException
	 */
    private ChannelSftp openAndConnectSFTPChannel(String oldPath, String newPath) throws IOException {
	    String CHANNEL_TYPE = "sftp";
        if (sshConnection.getSession() == null) {
            String msg = "SSH_DATACLIENT_SESSION_NULL_ERROR in method "+ this.getClass().getName() +" for user:  "
                    + username + " on destination host: "
                    + host + " old/current path :"+ oldPath + " newPath: " + newPath ;
            log.error(msg);
           throw new IOException(msg);
        }
           
        ChannelSftp channel = (ChannelSftp) sshConnection.openChannel(CHANNEL_TYPE );
        sshConnection.connectChannel(channel);
             
        if(channel == null) {
            String msg = "SSH_DATACLIENT_CHANNEL_NULL_ERROR in method "+ this.getClass().getName() +" for user:  "
                    + username + " on destination host: "
                    + host + " old/current path :"+ oldPath + " newPath: " + newPath ;
            log.error(msg);
           throw new IOException(msg);   
        }
        
        return  channel;
    }


}
