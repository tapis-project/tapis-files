package edu.utexas.tacc.tapis.files.lib.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;

import java.util.concurrent.TimeUnit;

public class RemoteClientCache {

    //  private static CacheLoader loader = new ClientSessionCacheLoader();
    private static final int CACHE_DURATION = 10;
    private static Cache<String, String> sessionCache =  CacheBuilder.newBuilder()
            .expireAfterWrite(CACHE_DURATION, TimeUnit.MINUTES)
            .build();
    private static final RemoteClientCache instance = new RemoteClientCache();

    public static Cache getCache() {
        return sessionCache;
    }

    public static RemoteClientCache getInstance() {
        return instance;
    }

    public static String get(String key){
        return sessionCache.getIfPresent(key);
    }

    public static void set(String key, String val){
        sessionCache.put(key, val);
    }

}
