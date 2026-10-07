package edu.utexas.tacc.tapis.files.lib.transfers;

/*
 * Static holder for runtime context that worker-side services need but that isn't
 * known until FilesApplication / TransfersApp finishes its startup sequence (e.g.
 * siteAdminTenantId, which depends on tenant/site data fetched from the Tenants
 * service).
 */
public class FilesAppContext
{
  private static String siteAdminTenantId;

  private FilesAppContext() {}

  public static synchronized void setSiteAdminTenantId(String siteAdminTenantId)
  {
    if (FilesAppContext.siteAdminTenantId != null)
    {
      throw new IllegalStateException("siteAdminTenantId has already been set");
    }
    FilesAppContext.siteAdminTenantId = siteAdminTenantId;
  }

  public static synchronized String getSiteAdminTenantId()
  {
    if (siteAdminTenantId == null)
    {
      throw new IllegalStateException("siteAdminTenantId has not been set");
    }
    return siteAdminTenantId;
  }
}
