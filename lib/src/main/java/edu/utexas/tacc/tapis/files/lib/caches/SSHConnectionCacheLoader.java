package edu.utexas.tacc.tapis.files.lib.caches;

import com.google.common.cache.CacheLoader;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.ssh.apache.system.TapisSSH;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSHConnectionCacheLoader extends CacheLoader<SSHConnectionCacheKey, SSHConnectionHolder>
{
  private static final Logger log = LoggerFactory.getLogger(SSHConnectionCacheLoader.class);

  @Override
  public SSHConnectionHolder load(SSHConnectionCacheKey key) throws TapisException, IllegalArgumentException
  {
    TapisSystem system = key.getSystem();
    TapisSSH tapisSSH = new TapisSSH(system);
    log.debug(LibUtils.getMsg("FILES_CACHE_CONN_LOADING", system.getTenant(), system.getId(), key.getEffUserId()));
    var c = new SSHConnectionHolder(tapisSSH.getConnection());
    log.debug(LibUtils.getMsg("FILES_CACHE_CONN_LOADED", system.getTenant(), system.getId(), key.getEffUserId()));
    return c;
  }
}
