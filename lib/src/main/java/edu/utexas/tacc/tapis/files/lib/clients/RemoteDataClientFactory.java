package edu.utexas.tacc.tapis.files.lib.clients;

import javax.validation.constraints.NotNull;

import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem.TransferMechanismsEnum;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RemoteDataClientFactory implements IRemoteDataClientFactory {

  @Override
  public IRemoteDataClient getRemoteDataClient(@NotNull TSystem system) throws IOException{

	   List<TransferMechanismsEnum> protocol = system.getTransferMechanisms();
	   if(protocol.contains(TransferMechanismsEnum.valueOf("SFTP"))) {
		   // FIXME: Remove the hardcoded password once the accessCredential is resolved in system Service
		   List<String> creds = new ArrayList<>();
		   creds.add("root");
		   system.setAccessCredential(creds);
		   return new SSHDataClient(system);
	   } else if (protocol.contains(TransferMechanismsEnum.valueOf("S3"))){
		   // FIXME: Remove the hardcorded password once the accessCredential is resolved in system Service
		   List<String> creds = new ArrayList<>();
		   creds.add("password");
		   system.setAccessCredential(creds);
		   return new S3DataClient(system);
	   } else {
		   throw new IOException("Invalid or protocol not supported");
	   }
	 
  }
}
