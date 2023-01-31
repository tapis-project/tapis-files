package edu.utexas.tacc.tapis.files.lib.caches;

import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Class to mark an SSHConnection as stale when it is removed from the cache.
 */
public class SSHConnectionExpire implements RemovalListener<SSHConnectionCacheKey, SSHConnectionHolder>
{
  private static final Logger log = LoggerFactory.getLogger(SSHConnectionExpire.class);
  @Override
  public void onRemoval(RemovalNotification<SSHConnectionCacheKey, SSHConnectionHolder> notification)
  {
    SSHConnectionHolder holder = notification.getValue();
    SSHConnectionCacheKey key = notification.getKey();
    TapisSystem sys = (key == null) ? null : key.getSystem();
    String tenant = (sys == null) ? null : sys.getTenant();
    String sysId = (sys == null) ? null : sys.getId();
    String effUser = (sys == null) ? null : sys.getEffectiveUserId();
    if (holder != null) holder.makeStale();
    String msg = LibUtils.getMsg("FILES_CLIENT_CONN_STALE", tenant, sysId, effUser);
    log.debug(msg);
  }
}
