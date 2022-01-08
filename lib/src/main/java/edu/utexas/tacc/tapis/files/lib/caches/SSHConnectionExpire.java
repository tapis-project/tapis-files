package edu.utexas.tacc.tapis.files.lib.caches;

import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Class to remove an SSHConnection from the cache and mark the connection as stale.
 */
public class SSHConnectionExpire implements RemovalListener<SSHConnectionCacheKey, SSHConnectionHolder>
{
  private static final Logger log = LoggerFactory.getLogger(SSHConnectionExpire.class);
  @Override
  public void onRemoval(RemovalNotification<SSHConnectionCacheKey, SSHConnectionHolder> notification)
  {
    SSHConnectionHolder holder = notification.getValue();
    SSHConnectionCacheKey key = notification.getKey();
    holder.makeStale();
    // TODO: move msg to resource bundle
    log.debug("Expired SSH connecting from cache for System: {} EffectiveUser {}", key.getSystem().getId(), key.getEffUserId());
//    SSHConnectionHolder holder = notification.getValue();
//    SSHConnectionCacheKey key = notification.getKey();
//    log.debug("Closing SSH connecting from cache for System: {} EffectiveUser {}", key.getSystem().getId(), key.getEffUserId());
//    holder.getSshConnection().stop();
//    log.debug("Closed SSH connecting from cache for System: {} EffectiveUser {}", key.getSystem().getId(), key.getEffUserId());
  }
}
