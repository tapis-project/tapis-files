package edu.utexas.tacc.tapis.files.lib.clients;

import javax.validation.constraints.NotNull;

import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

import java.io.IOException;

public interface IRemoteDataClientFactory
{
   IRemoteDataClient getRemoteDataClient(@NotNull String apiTenant, @NotNull String apiUser,
                                         @NotNull TapisSystem system, @NotNull String effUserId) throws IOException;
}
