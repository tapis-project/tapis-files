package edu.utexas.tacc.tapis.files.lib.Kernel;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.files.lib.exceptions.FilesKernelException;
import edu.utexas.tacc.tapis.files.lib.kernel.SftpFilesKernel;

import org.testng.Assert;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

@Test(groups = { "integration" })
public class FilesKernelIntegrationTests {
	/**
	 * This test class is used to validate that the functioning of Files Kernel
	 * Private and Public keys are needed as arguments if Public authentication
	 * needs to be validated Password is needed as an argument to validate the
	 * password authentication method
	 * 
	 *
	 */

	private byte[] privateKey;
	private byte[] publicKey ;
	private byte[] passphrase;
	private String identity;
	
	// Local logger.
	private static final Logger _log = LoggerFactory.getLogger(FilesKernelIntegrationTests.class.getName());
	
	@BeforeSuite
	private void readPrivateKey () throws IOException {
		
		File file1 = new File(this.getClass().getResource("/ssh-machine").getFile());
		String privateKeyFile = file1.getAbsolutePath();	
		Path path = Paths.get(privateKeyFile);
		privateKey = Files.readAllBytes(path);
	    publicKey = null;	    
		passphrase = null;
	    identity = "sftp";	   
		
	}
		
	/* ********************************************************************** */
	/* Integration tests for Public Authentication */
	/* ********************************************************************** */

	/**
	 * This is a positive test is for sftp file transfer when public auth method is
	 * used This returns a success message if the file is transfered File size is
	 * 18.5MB
	 * 
	 * @throws FilesKernelException
	 * @throws IOException 
	 */
	
	@Test
	private void fileTransferKeySuccessTest() throws FilesKernelException {
		String user = "root";
		String host = "localhost";
		String port = "2222";		
	    File file = new File(this.getClass().getResource("/test.tar").getFile());
	    String LOCALFILE = file.getAbsolutePath();
		String REMOTEFILE = "/root/tmp";
		SftpFilesKernel sftp = new SftpFilesKernel(host, user, Integer.parseInt(port), privateKey, publicKey, passphrase, identity);
		sftp.initSession();
		String actual = sftp.transferFile(LOCALFILE, REMOTEFILE);
		String expected = "Successfully transfered file";
		Assert.assertEquals(actual, expected);

	}

	/**
	 * This is a positive test is for sftp file transfer when public auth method
	 * is used This returns a success message if the file is transfered File size is
	 * 0KB
	 * 
	 * @throws FilesKernelException
	 */
	
	@Test
	private void fileTransferSuccessKeyEmptyFileTest() throws FilesKernelException {
		String user = "root";
		String host = "localhost";
		String port = "2222";
	    File file = new File(this.getClass().getResource("/test1.txt").getFile());
	    String LOCALFILE = file.getAbsolutePath();
		String REMOTEFILE = "/root/tmp";
		SftpFilesKernel sftp = new SftpFilesKernel(host, user, Integer.parseInt(port), privateKey, publicKey, passphrase, identity);
		sftp.initSession();
		String actual = sftp.transferFile(LOCALFILE, REMOTEFILE);
		String expected = "Successfully transfered file";
		Assert.assertEquals(actual, expected);


	}
	
	/**
	 * This is a negative test is for sftp file transfer when public auth method is
	 * used A wrong port number is provided in this test expected exception is
	 * FilesKernelException
	 */
	@Test(expectedExceptions = FilesKernelException.class)
	private void fileTransferNegKeyPortTest() throws FilesKernelException {
		String user = "root";
		String host = "localhost";
		String port = "22";
	    File file = new File(this.getClass().getResource("/test1.txt").getFile());
	    String LOCALFILE = file.getAbsolutePath();
		String REMOTEFILE = "/root/tmp";
		SftpFilesKernel sftp = new SftpFilesKernel(host, user, Integer.parseInt(port), privateKey, publicKey, passphrase, identity);
		sftp.initSession();
		String actual = sftp.transferFile(LOCALFILE, REMOTEFILE);
	}

	/**
	 * This is a negative test is for sftp file transfer when public auth method is
	 * used An invalid user is provided in this test expected exception is
	 * FilesKernelException
	 */
	@Test(expectedExceptions = FilesKernelException.class)
	private void fileTransferNegKeyUserTest() throws FilesKernelException {
		// admin is not a valid user
		String user = "admin";
		String host = "localhost";
		String port = "2222";
	    File file = new File(this.getClass().getResource("/test1.txt").getFile());
	    String LOCALFILE = file.getAbsolutePath();
		String REMOTEFILE = "/root/tmp";
		SftpFilesKernel sftp = new SftpFilesKernel(host, user, Integer.parseInt(port), privateKey, publicKey, passphrase, identity);
		sftp.initSession();
		String actual = sftp.transferFile(LOCALFILE, REMOTEFILE);

	}

	/* ********************************************************************** */
	/* Integration tests for Password Authentication */
	/* ********************************************************************** */

	/**
	 * This is a positive test is for sftp file transfer when password auth method
	 * is used This returns a success message if the file is transfered File size is
	 * 18.5MB
	 * 
	 * @throws FilesKernelException
	 */
	
	@Test
	private void fileTransferPasswordSuccessTest() throws FilesKernelException {
		String user = "root";
		String host = "localhost";
		String port = "2222";
		String password = "root";
		File file = new File(this.getClass().getResource("/test.tar").getFile());
	    String LOCALFILE = file.getAbsolutePath();
		String REMOTEFILE = "/root/tmp";
		SftpFilesKernel sftp = new SftpFilesKernel(host, Integer.parseInt(port), user, password);
		sftp.initSession();
		String actual = sftp.transferFile(LOCALFILE, REMOTEFILE);
		String expected = "Successfully transfered file";
		Assert.assertEquals(actual, expected);

	}

	/**
	 * This is a positive test is for sftp file transfer when password auth method
	 * is used This returns a success message if the file is transfered File size is
	 * 0KB
	 * 
	 * @throws FilesKernelException
	 */
	@Test
	private void fileTransferSuccessPasswordEmptyFileTest() throws FilesKernelException {
		String user = "root";
		String host = "localhost";
		String port = "2222";
		String password = "root";
		System.out.println("Running test fileTransferSuccessPasswordEmptyFileTest " );
	    File file = new File(this.getClass().getResource("/test1.txt").getFile());
	    String LOCALFILE = file.getAbsolutePath();
		String REMOTEFILE = "/root/tmp";
		SftpFilesKernel sftp = new SftpFilesKernel(host, Integer.parseInt(port), user, password);
		sftp.initSession();
		String actual = sftp.transferFile(LOCALFILE, REMOTEFILE);
		String expected = "Successfully transfered file";
		Assert.assertEquals(actual, expected);
	}

	/**
	 * This is a negative test is for sftp file transfer for password auth method
	 * A wrong port number is provided in this test 
	 * Expected exception is FilesKernelException
	 * 
	 */
	
	@Test(expectedExceptions = FilesKernelException.class)
	private void fileTransferNegPasswordPortTest() throws FilesKernelException {
		String user = "root";
		String host = "localhost";
		String port = "22";
		String password = "root";
		File file = new File(this.getClass().getResource("/test1.txt").getFile());
	    String LOCALFILE = file.getAbsolutePath();
		String REMOTEFILE = "/root/tmp";
		SftpFilesKernel sftp = new SftpFilesKernel(host, Integer.parseInt(port), user, password);
		sftp.initSession();
		String actual = sftp.transferFile(LOCALFILE, REMOTEFILE);

	}

	
	/**
	 * This is a negative test is for sftp file transfer when password auth method
	 * is used An invalid user is provided in this test expected exception is
	 * FilesKernelException
	 */
	@Test(expectedExceptions = FilesKernelException.class)
	private void fileTransferNegPasswordUserTest() throws FilesKernelException {
		// admin is not a valid user
		String user = "admin";
		String host = "localhost";
		String port = "2222";
		String password = "root";
		File file = new File(this.getClass().getResource("/test1.txt").getFile());
	    String LOCALFILE = file.getAbsolutePath();
		String REMOTEFILE = "/root/tmp";
		SftpFilesKernel sftp = new SftpFilesKernel(host, Integer.parseInt(port), user, password);
		sftp.initSession();
		String actual = sftp.transferFile(LOCALFILE, REMOTEFILE);

	}
	
	/**
	 * This is a negative test is for sftp file transfer when password auth method
	 * is used An invalid localpath is provided in this test expected exception is
	 * FilesKernelException
	 */
	@Test(expectedExceptions = FilesKernelException.class)
	private void fileTransferNegLocalTest() throws FilesKernelException {
		// admin is not a valid user
		String user = "root";
		String host = "localhost";
		String port = "2222";
		String password = "root";
		String LOCALFILE = "//home/test";
		String REMOTEFILE = "/root/tmp";
		SftpFilesKernel sftp = new SftpFilesKernel(host, Integer.parseInt(port), user, password);
		sftp.initSession();
		sftp.transferFile(LOCALFILE, REMOTEFILE);

	}

	/**
	 * This is a negative test is for sftp file transfer when password auth method
	 * is used An invalid remote path is provided in this test expected exception is
	 * FilesKernelException
	 */
	@Test(expectedExceptions = FilesKernelException.class)
	private void fileTransferNegRemoteTest() throws FilesKernelException {
		// admin is not a valid user
		String user = "admin";
		String host = "localhost";
		String port = "2222";
		String password = "root";
		File file = new File(this.getClass().getResource("/test1.txt").getFile());
	    String LOCALFILE = file.getAbsolutePath();
		String REMOTEFILE = "/root/tmp12/test";
		SftpFilesKernel sftp = new SftpFilesKernel(host, Integer.parseInt(port), user, password);
		sftp.initSession();
		sftp.transferFile(LOCALFILE, REMOTEFILE);

	}
	

}
