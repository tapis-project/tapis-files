package edu.utexas.tacc.tapis.files.lib.clients;

import java.io.IOException;
import javax.validation.constraints.NotNull;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

public interface IRemoteDataClientFactory
{
   IRemoteDataClient getRemoteDataClient(@NotNull String oboTenant, @NotNull String oboUser,
                                         @NotNull TapisSystem system)
           throws IOException;
}
