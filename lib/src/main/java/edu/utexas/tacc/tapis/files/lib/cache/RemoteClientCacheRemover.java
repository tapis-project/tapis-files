package edu.utexas.tacc.tapis.files.lib.cache;

import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;

public class RemoteClientCacheRemover implements RemovalListener<String, IRemoteDataClient> {

    @Override
    public void onRemoval(RemovalNotification<String, IRemoteDataClient> removalNotification) {
        IRemoteDataClient client = removalNotification.getValue();
        client.disconnect();
    }
}
