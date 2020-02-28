package edu.utexas.tacc.tapis.files.lib.clients;

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
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem.AccessMethodEnum;

import javax.inject.Named;

public class SSHDataClient implements IRemoteDataClient {
    
    private Logger log = LoggerFactory.getLogger(SSHDataClient.class);
	String host;
	int port;
	String username;
	String password;
	String path ;
	AccessMethodEnum accessMethod;
	String remotePath;
	SftpFilesKernel sftp;
	String rootDir;
	
	
	public SSHDataClient(TSystem system) {
		host = system.getHost();
		port = system.getPort();
		username = system.getEffectiveUserId();
		password = system.getAccessCredential().getPassword();
		remotePath = system.getBucketName();
		accessMethod = system.getAccessMethod();
		rootDir = system.getRootDir();
		
	}
	@Override
	public List<FileInfo> ls(String remotePath) throws IOException {
		List<FileInfo> fileListing = new ArrayList<>();
		try {
			 Path remoteAbsolutePath = Paths.get(rootDir,remotePath);
			 fileListing = sftp.ls(remoteAbsolutePath.toString());
		} catch (FilesKernelException e) {
			// TODO Auto-generated catch block
			log.error("SSH listing error", e);
			throw new IOException("Error listing system/path");
		}
		return fileListing;
	}

	@Override
	public void mkdir(String remotePath) throws IOException {
	    	        
            Path remoteAbsolutePath = Paths.get(rootDir,remotePath);
            log.debug("SSHDataClient: mkdir call abs path: " + remoteAbsolutePath);
        try { 
            String mkdirStatus = sftp.mkdir(remoteAbsolutePath.toString());
            log.debug("File mkdir status from remote execution: " + mkdirStatus);
            
       } catch (FilesKernelException e) {
          throw new IOException("mkdir failure", e);
       }

	}

	@Override
	public void insert(String remotePath, InputStream fileStream) throws IOException {

	}

	@Override
	public void move(String oldPath, String newPath) throws IOException {
		// TODO Auto-generated method stub

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
