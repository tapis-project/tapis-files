package edu.utexas.tacc.tapis.files.lib.cache;

import com.google.common.cache.CacheLoader;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;

public class RemoteClientCacheLoader extends CacheLoader<String, IRemoteDataClient> {

    @Override
    public IRemoteDataClient load(String s) throws Exception {
        return null;
    }
}
