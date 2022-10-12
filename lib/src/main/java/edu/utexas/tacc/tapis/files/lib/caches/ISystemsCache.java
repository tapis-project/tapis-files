package edu.utexas.tacc.tapis.files.lib.caches;

import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

/*
 * SystemsCache interface. Allows for using SystemsCache and SystemsCacheNoAuth interchangeably.
 */
public interface ISystemsCache
{
  public TapisSystem getSystem(String tenantId, String systemId, String username) throws ServiceException;
}
