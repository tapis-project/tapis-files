package edu.utexas.tacc.tapis.files.lib.caches;

import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

import java.util.Objects;

/**
 * Cache key for the LoadingCache. Equality is based on tenantId, systemId and effUserId.
 * Note that tenantId and systemId are contained in the TapisSystem
 */
public class SSHConnectionCacheKey
{
  private final TapisSystem system;
  private final String effUserId;

  public SSHConnectionCacheKey(TapisSystem system1, String effUserId1)
  {
    system = system1;
    effUserId = effUserId1;
  }

  public TapisSystem getSystem() {
    return system;
  }

  public String getEffUserId() {
    return effUserId;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SSHConnectionCacheKey that = (SSHConnectionCacheKey) o;
    return Objects.equals(this.system.getId(), that.system.getId()) &&
            Objects.equals(this.effUserId, that.getEffUserId()) &&
            Objects.equals(this.system.getTenant(), that.system.getTenant());
  }

  @Override
  public int hashCode() {
    return Objects.hash(system.getId(), system.getTenant(), effUserId);
  }
}
