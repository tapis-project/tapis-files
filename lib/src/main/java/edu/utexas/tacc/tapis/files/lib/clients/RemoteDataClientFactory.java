package edu.utexas.tacc.tapis.files.lib.clients;

import javax.validation.constraints.NotNull;

import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem.TransferMethodsEnum;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RemoteDataClientFactory implements IRemoteDataClientFactory {

  @Override
  public IRemoteDataClient getRemoteDataClient(@NotNull TSystem system) throws IOException{

	   List<TransferMethodsEnum> protocol = system.getTransferMethods();
	   Credential creds = new Credential();
	   if(protocol.contains(TransferMethodsEnum.valueOf("SFTP"))) {
		   // FIXME: Remove the hardcoded password once the accessCredential is resolved in system Service
	       creds.setPassword("root");
		   //creds.setAccessKey("root");
		   system.setAccessCredential(creds);
		   return new SSHDataClient(system);
	   } else if (protocol.contains(TransferMethodsEnum.valueOf("S3"))){
		   return new S3DataClient(system);
	   } else {
		   throw new IOException("Invalid or protocol not supported");
	   }
	 
  }
}
