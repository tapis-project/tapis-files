package edu.utexas.tacc.tapis.files.lib.clients;

import javax.validation.constraints.NotNull;

import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

import java.io.IOException;

public interface IRemoteDataClientFactory
{
   IRemoteDataClient getRemoteDataClient(@NotNull String oboTenant, @NotNull String oboUser,
                                         @NotNull TapisSystem system, @NotNull String username) throws IOException;
}
