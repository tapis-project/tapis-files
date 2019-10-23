package edu.utexas.tacc.tapis.files.lib.clients;

import javax.validation.constraints.NotNull;
import java.io.IOException;

public class RemoteDataClientFactory implements IRemoteDataClientFactory {

  @Override
  public IRemoteDataClient getRemoteDataClient(@NotNull FakeSystem system) throws IOException{

    if (system.getProtocol().equals("S3")) {
        return new S3DataClient(system);
    } else {
      throw new IOException("Invalid protocol");
    }

  }
}
