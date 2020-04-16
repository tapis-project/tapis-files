package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem.TransferMethodsEnum;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.List;

public class RemoteDataClientFactory implements IRemoteDataClientFactory {

  @Override
  public IRemoteDataClient getRemoteDataClient(@NotNull TSystem system) throws IOException{

	   List<TransferMethodsEnum> protocol = system.getTransferMethods();
	   if(protocol.contains(TransferMethodsEnum.valueOf("SFTP"))) {
		   return new SSHDataClient(system);
	   } else if (protocol.contains(TransferMethodsEnum.valueOf("S3"))){
		   return new S3DataClient(system);
	   } else {
		   throw new IOException("Invalid or protocol not supported");
	   }
	 
  }
}
