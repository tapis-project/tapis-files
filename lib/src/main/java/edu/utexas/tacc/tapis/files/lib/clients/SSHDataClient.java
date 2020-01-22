package edu.utexas.tacc.tapis.files.lib.clients;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import edu.utexas.tacc.tapis.files.lib.exceptions.FilesKernelException;
import edu.utexas.tacc.tapis.files.lib.kernel.SftpFilesKernel;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem.AccessMethodEnum;

public class SSHDataClient implements IRemoteDataClient {
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
		password = system.getAccessCredential().getPassword().get(0);
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
			 System.out.println("File Lists from remote execution" + fileListing);
			 
		} catch (FilesKernelException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return fileListing;
	}

	@Override
	public void mkdir(String remotePath) throws IOException {
	    try {
            Path remoteAbsolutePath = Paths.get(rootDir,remotePath);
            String mkdirStatus = sftp.mkdir(remoteAbsolutePath.toString());
            System.out.println("File mkdir from remote execution" + mkdirStatus);
            
       } catch (FilesKernelException e) {
           // TODO Auto-generated catch block
           e.printStackTrace();
       }

	}

	@Override
	public void insert(String remotePath, InputStream fileStream) throws IOException {

	}

	@Override
	public void move(String oldPath, String newPath) {
		// TODO Auto-generated method stub

	}

	@Override
	public void copy(String currentPath, String newPath) {
		// TODO Auto-generated method stub

	}

	@Override
	public void delete(String path) {
		// TODO Auto-generated method stub

	}

	@Override
	public InputStream getStream(String path) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void download(String path) {
		// TODO Auto-generated method stub

	}

	@Override
	public void connect() {
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

}
