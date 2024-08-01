package edu.utexas.tacc.tapis.files.lib.clients;

import java.io.IOException;
import javax.validation.constraints.NotNull;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

public interface IRemoteDataClientFactory
{
   // Some methods do not support impersonationId or sharedAppCtxGrantor
   public static final String IMPERSONATION_ID_NULL = null;
   public static final String SHARED_CTX_GRANTOR_NULL = null;


   IRemoteDataClient getRemoteDataClient(@NotNull String oboTenant, @NotNull String oboUser,
                                         @NotNull TapisSystem system, String impersonationId,
                                         String sharedCtxGrantor)
   throws IOException;
}
