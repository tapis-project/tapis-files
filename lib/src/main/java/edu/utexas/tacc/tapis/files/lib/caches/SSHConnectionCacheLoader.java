package edu.utexas.tacc.tapis.files.lib.caches;

import com.google.common.cache.CacheLoader;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.ssh.apache.system.TapisSSH;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

public class SSHConnectionCacheLoader extends CacheLoader<SSHConnectionCacheKey, SSHConnectionHolder>
{
  @Override
  public SSHConnectionHolder load(SSHConnectionCacheKey key) throws TapisException, IllegalArgumentException
  {
    TapisSystem system = key.getSystem();
    TapisSSH tapisSSH = new TapisSSH(system);
    return new SSHConnectionHolder(tapisSSH.getConnection());
  }
}
