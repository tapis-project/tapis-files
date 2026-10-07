package edu.utexas.tacc.tapis.files.lib.transfers;

import java.lang.reflect.Field;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class TestTransferAppContext
{
  @AfterMethod
  public void resetSiteAdminTenantId() throws Exception
  {
    Field field = FilesAppContext.class.getDeclaredField("siteAdminTenantId");
    field.setAccessible(true);
    field.set(null, null);
  }

  @Test
  public void testGetBeforeSetThrows()
  {
    Assert.assertThrows(IllegalStateException.class, FilesAppContext::getSiteAdminTenantId);
  }

  @Test
  public void testSetThenGetReturnsValue()
  {
    FilesAppContext.setSiteAdminTenantId("admin");
    Assert.assertEquals(FilesAppContext.getSiteAdminTenantId(), "admin");
  }

  @Test
  public void testSetTwiceThrows()
  {
    FilesAppContext.setSiteAdminTenantId("admin");
    Assert.assertThrows(IllegalStateException.class, () -> FilesAppContext.setSiteAdminTenantId("other"));
  }
}
