package edu.utexas.tacc.tapis.files.lib.caches;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import edu.utexas.tacc.tapis.security.client.SKClient;
import org.apache.commons.lang3.StringUtils;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo.Permission;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;

@Service
public class FilePermsCache extends BasePermsCache
{
  private static final Logger log = LoggerFactory.getLogger(FilePermsCache.class);

  private final LoadingCache<FilePermCacheKey, Boolean> cache;
  // PERMSPEC is "files:tenant:READ,MODIFY:systemId:path
  private static final String PERMSPEC = "files:%s:%s:%s:%s";

  @Inject
  public FilePermsCache()
  {
    cache = CacheBuilder.newBuilder().expireAfterWrite(Duration.ofSeconds(60)).build(new PermsLoader());
  }

  /**
   * Return Permission or null if no permission granted
   *
   * @param tenantId tenantId
   * @param username username
   * @param systemId ID of system
   * @param path path to file/folder
   * @return Permission - null if no permission granted
   * @throws ServiceException IF SK call failed
   */
  public boolean checkPerm(String tenantId, String username, String systemId, String path, Permission perm)
          throws ServiceException
  {
    try {
      // 99% of the time, a user will have access to the root of the storage system, so lets
      // just check that first. If they have access to the top level, anything below is also permitted.
      FilePermCacheKey topKey = new FilePermCacheKey(tenantId, systemId, "/", username, perm);
      boolean hasTopAccess = cache.get(topKey);
      if (hasTopAccess) return true;
      // If that didn't catch it, lets check for the true path given.
      FilePermCacheKey key = new FilePermCacheKey(tenantId, systemId, path, username, perm);
      return cache.get(key);
    } catch (ExecutionException ex) {
      String msg = LibUtils.getMsg("FILES_CACHE_ERR", "FilePerms", tenantId, systemId, username, ex.getMessage());
      throw new ServiceException(msg, ex);
    }
  }

  public void invalidateSystemPerms(String tenantId, String systemId) {
    invalidatePerms(key -> {
      if((StringUtils.equals(tenantId, key.getTenantId())) &&
              (StringUtils.equals(systemId, key.getSystemId()))) {
        return true;
      }
      return false;
    });
  }

  public void invalidateUserPerms(String tenantId, String username, String systemId) {
    invalidatePerms(key -> {
      if((StringUtils.equals(tenantId, key.getTenantId())) &&
              (StringUtils.equals(systemId, key.getSystemId())) &&
              (StringUtils.equals(username, key.getUsername()))) {
        return true;
      }
      return false;
    });
  }

  private void invalidatePerms(Predicate<FilePermCacheKey> keyInvalidator) {
    Set<FilePermCacheKey> keysToInvalidate = cache.asMap().keySet().stream().
            filter(keyInvalidator).collect(Collectors.toSet());
    cache.invalidateAll(keysToInvalidate);
  }

  /**
   * Return Permission or null if no permission granted
   *
   * @param tenantId tenantId
   * @param username username
   * @param systemId ID of system
   * @param path path to file/folder
   * @return Permission - null if no permission granted
   * @throws ServiceException IF SK call failed
   */
  public Permission fetchPerm(String tenantId, String username, String systemId, String path) throws ServiceException
  {
    try {
      FilePermCacheKey key = new FilePermCacheKey(tenantId, systemId, path, username, Permission.MODIFY);
      if (cache.get(key)) return Permission.MODIFY;
      key = new FilePermCacheKey(tenantId, systemId, path, username, Permission.READ);
      if (cache.get(key)) return Permission.READ;
      return null;
    } catch (ExecutionException ex) {
      String msg = LibUtils.getMsg("FILES_CACHE_ERR", "FilePerms", tenantId, systemId, username, ex.getMessage());
      throw new ServiceException(msg, ex);
    }
  }

  // ====================================================================================
  // =======  Private Classes ===========================================================
  // ====================================================================================

  /**
   * Class implementing method needed for populating the cache.
   */
  private class PermsLoader extends CacheLoader<FilePermCacheKey, Boolean>
  {
    @Override
    public Boolean load(FilePermCacheKey key) throws Exception
    {
      log.debug(LibUtils.getMsg("FILES_CACHE_PERM_LOADING", key.getTenantId(), key.getSystemId(), key.getUsername(),
                                key.getPerm().name(), key.getPath()));
      String permSpec = String.format(PERMSPEC, key.getTenantId(), key.getPerm(), key.getSystemId(), key.getPath());
      SKClient skClient = getSKClient();
      skClient.setReadTimeout(30000);
      skClient.setConnectTimeout(30000);
      boolean isPermitted = skClient.isPermitted(key.getTenantId(), key.getUsername(), permSpec);
      log.debug(LibUtils.getMsg("FILES_CACHE_PERM_LOADED", key.getTenantId(), key.getSystemId(), key.getUsername(),
                                key.getPerm().name(), key.getPath()));
      return isPermitted;
    }
  }

  /**
   * Class representing the cache key.
   * Unique keys for tenantId+user+systemId+path
   */
  private static class FilePermCacheKey
  {
    private final String tenantId;
    private final String username;
    private final String systemId;
    private final String path;
    private final Permission perm;

    public FilePermCacheKey(String tenantId, String systemId, String path, String username, Permission perm)
    {
      this.systemId = systemId;
      this.tenantId = tenantId;
      this.path = path;
      this.username = username;
      this.perm = perm;
    }

    // ====================================================================================
    // =======  Accessors =================================================================
    // ====================================================================================
    public String getTenantId() {
      return tenantId;
    }
    public String getSystemId() {
      return systemId;
    }
    public String getPath() {
      return path;
    }
    public String getUsername() {
      return username;
    }
    public Permission getPerm() {
      return perm;
    }

    // ====================================================================================
    // =======  Support for equals ========================================================
    // ====================================================================================
    @Override
    public boolean equals(Object o)
    {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      FilePermCacheKey that = (FilePermCacheKey) o;
      if (!tenantId.equals(that.tenantId)) return false;
      if (!username.equals(that.username)) return false;
      if (!systemId.equals(that.systemId)) return false;
      if (!path.equals(that.path)) return false;
      return perm == that.perm;
    }

    @Override
    public int hashCode()
    {
      int result = tenantId.hashCode();
      result = 31 * result + username.hashCode();
      result = 31 * result + systemId.hashCode();
      result = 31 * result + path.hashCode();
      result = 31 * result + perm.hashCode();
      return result;
    }
  }


}
