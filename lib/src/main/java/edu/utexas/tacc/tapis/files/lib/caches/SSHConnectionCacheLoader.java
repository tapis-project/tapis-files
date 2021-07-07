package edu.utexas.tacc.tapis.files.lib.caches;

import com.google.common.cache.CacheLoader;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHConnection;
import edu.utexas.tacc.tapis.shared.ssh.apache.system.TapisSSH;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSHConnectionCacheLoader extends CacheLoader<SSHConnectionCacheKey, SSHConnectionHolder> {

    private static final Logger log = LoggerFactory.getLogger(SSHConnectionCache.class);

    @Override
    public SSHConnectionHolder load(SSHConnectionCacheKey key) throws TapisException, IllegalArgumentException {
        TapisSystem system = key.getSystem();
        String username = key.getUsername();
        TapisSSH tapisSSH = new TapisSSH(system);
        return new SSHConnectionHolder(tapisSSH.getConnection());
    }
}
