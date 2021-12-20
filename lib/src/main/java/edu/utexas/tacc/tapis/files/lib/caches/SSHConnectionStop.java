package edu.utexas.tacc.tapis.files.lib.caches;

import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Class to stop an SSHConnection when it is removed from the cache
 */
public class SSHConnectionStop implements RemovalListener<SSHConnectionCacheKey, SSHConnectionHolder>
{
  private static final Logger log = LoggerFactory.getLogger(SSHConnectionStop.class);
  @Override
  public void onRemoval(RemovalNotification<SSHConnectionCacheKey, SSHConnectionHolder> notification)
  {
    SSHConnectionHolder holder = notification.getValue();
    SSHConnectionCacheKey key = notification.getKey();
    log.debug("Closing SSH connecting from cache for System: {} User {}", key.getSystem().getId(), key.getUsername());
    holder.getSshConnection().stop();
    log.debug("Closed SSH connecting from cache for System: {} User {}", key.getSystem().getId(), key.getUsername());
  }
}
