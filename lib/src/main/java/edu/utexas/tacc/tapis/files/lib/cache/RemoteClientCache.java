package edu.utexas.tacc.tapis.files.lib.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


/*
    This class stores a cache of remote data clients. By default, if the client is not accessed in 1 minute,
    the client will be remove from the cache. This prevents rapid connection requests which are costly operations.
*/
public class RemoteClientCache {

    private static final int CACHE_DURATION = 1;
    private static LoadingCache<String, IRemoteDataClient> clientCache =  CacheBuilder.newBuilder()
            .expireAfterAccess(CACHE_DURATION, TimeUnit.MINUTES)
            .removalListener(new RemoteClientCacheRemover())
            .build(new RemoteClientCacheLoader());

    public static IRemoteDataClient get(String key) throws ServiceException {
        try {
            return clientCache.get(key);
        } catch (ExecutionException ex) {
            throw new ServiceException("Could not get client " + key);
        }
    }


}
