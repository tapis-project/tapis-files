package edu.utexas.tacc.tapis.files.lib.clients;

import javax.validation.constraints.NotNull;
import java.io.IOException;

public interface IRemoteDataClientFactory {

  IRemoteDataClient getRemoteDataClient(@NotNull FakeSystem system) throws IOException;


}
