package edu.utexas.tacc.tapis.files.lib.cache;

import com.google.common.cache.CacheLoader;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;

import javax.validation.constraints.NotNull;

public class RemoteClientCacheLoader extends CacheLoader<String, IRemoteDataClient> {

    @Override
    public IRemoteDataClient load(@NotNull String systemId) throws Exception {
        return null;
    }
}
