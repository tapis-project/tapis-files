package edu.utexas.tacc.tapis.files.lib.clients;

import javax.validation.constraints.NotNull;

import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;

import java.io.IOException;

public interface IRemoteDataClientFactory {

   IRemoteDataClient getRemoteDataClient(@NotNull TSystem system, @NotNull String username) throws IOException;


}
