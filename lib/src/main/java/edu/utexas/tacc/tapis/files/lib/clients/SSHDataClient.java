package edu.utexas.tacc.tapis.files.lib.clients;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.utexas.tacc.tapis.files.lib.exceptions.FilesKernelException;
import edu.utexas.tacc.tapis.files.lib.kernel.RemoteExecFilesKernel;
import edu.utexas.tacc.tapis.files.lib.kernel.SftpFilesKernel;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem.AccessMechanismEnum;

public class SSHDataClient implements IRemoteDataClient {
	String host;
	int port;
	String username;
	String password;
	String path ;
	AccessMechanismEnum accessMechanism;
	String remotePath;
	SftpFilesKernel sftp;
	
	/*public SSHDataClient(FakeSystem system) {
		host = system.getHost();
		port = system.getPort();
		username = system.getUsername();
		password = system.getPassword();
		remotePath = system.getBucketName();
		accessMechanism = system.getAccessMechanism();
			
	}*/
	
	public SSHDataClient(TSystem system) {
		host = system.getHost();
		port = system.getPort();
		username = system.getEffectiveUserId();
		password = system.getAccessCredential();
		remotePath = system.getBucketName();
		accessMechanism = system.getAccessMechanism();
			
	}
	@Override
	public List<FileInfo> ls(String remotePath) throws IOException {
		List<FileInfo> fileListing = new ArrayList<FileInfo>();
		try {
			 fileListing = sftp.ls(remotePath);
			System.out.println("File Lists from remote execution" + fileListing);
		} catch (FilesKernelException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return fileListing;
	}

	@Override
	public void move(String srcSystem, String srcPath, String destSystem, String destPath) {
		// TODO Auto-generated method stub

	}

	@Override
	public void rename() {
		// TODO Auto-generated method stub

	}

	@Override
	public void copy() {
		// TODO Auto-generated method stub

	}

	@Override
	public void delete() {
		// TODO Auto-generated method stub

	}

	@Override
	public void getStream() {
		// TODO Auto-generated method stub

	}

	@Override
	public void download() {
		// TODO Auto-generated method stub

	}

	@Override
	public void connect() {
		// TODO Auto-generated method stub
		switch(accessMechanism.getValue()) {
		case "SSH_PASSWORD":
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
		// TODO Auto-generated method stub

	}

}
