package edu.utexas.tacc.tapis.files.lib.clients;

import javax.validation.constraints.NotNull;

import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem.TransferMechanismsEnum;

import java.io.IOException;
import java.util.List;

public class RemoteDataClientFactory implements IRemoteDataClientFactory {

  @Override
  //public IRemoteDataClient getRemoteDataClient(@NotNull FakeSystem system) throws IOException{
  public IRemoteDataClient getRemoteDataClient(@NotNull TSystem system) throws IOException{

	   List<TransferMechanismsEnum> protocol = system.getTransferMechanisms();
	   if(protocol.contains(TransferMechanismsEnum.valueOf("SFTP"))) {
		   // FIXME: Remove the hardcoded password once the accessCredential is resolved in system Service
		   system.setAccessCredential("root");
		   return new SSHDataClient(system);
	   } else if (protocol.contains(TransferMechanismsEnum.valueOf("S3"))){
		   // FIXME: Remove the hardcorded password once the accessCredential is resolved in system Service
		   system.setAccessCredential("password");
		   return new S3DataClient(system);
	   } else {
		   throw new IOException("Invalid or protocol not supported");
	   }
	 
  }
}
