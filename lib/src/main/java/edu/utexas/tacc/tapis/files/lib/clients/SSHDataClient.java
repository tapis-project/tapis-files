package edu.utexas.tacc.tapis.files.lib.clients;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import edu.utexas.tacc.tapis.files.lib.exceptions.FilesKernelException;
import edu.utexas.tacc.tapis.files.lib.kernel.SftpFilesKernel;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;

import javax.inject.Named;

public class SSHDataClient implements IRemoteDataClient {
    
    private Logger log = LoggerFactory.getLogger(SSHDataClient.class);
	String host;
	int port;
	String username;
	String password;
	String path ;
	TSystem.DefaultAccessMethodEnum accessMethod;
	String remotePath;
	SftpFilesKernel sftp;
	String rootDir;
	String systemId;
	
	
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
	@Override
	public List<FileInfo> ls(String remotePath) throws IOException {
		List<FileInfo> filesListing = new ArrayList<>();
		Path remoteAbsolutePath = null;
		try {
			 remoteAbsolutePath = Paths.get(rootDir,remotePath);
			 filesListing = sftp.ls(remoteAbsolutePath.toString());
		} catch (Exception e) {
		    String msg = "SSH_LISTING_ERROR for system " + systemId + " path: " + remoteAbsolutePath 
		                  + ": " + e.getMessage();
			log.error(msg, e);
			throw new IOException("File Listing Failed" + msg);
		} 
		return filesListing;
	}

	@Override
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

	}

	@Override
	public void insert(String remotePath, InputStream fileStream) throws IOException {

	}

	@Override
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
		// TODO Auto-generated method stub
		switch(accessMethod.getValue()) {
		case "PASSWORD":
			System.out.println("host: "+ host+ " port: "+ port+ " username: "+ username + " password: "+ password);
			sftp = new SftpFilesKernel(host, port, username, password);
			try {
				sftp.initSession();
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
		sftp.closeSession();

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
