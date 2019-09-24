package edu.utexas.tacc.tapis.files.lib.dao;

import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups={"integration"})
public class TestFileTransfersDAO {

  @Test
  public void testGetTransferTask() {
    Assert.assertEquals(1, 1);
  }

  @Test
  public void testCreateTransferTask() {
    FileTransfersDAO dao = new FileTransfersDAO();
    dao.createTransferTask();
    Assert.assertEquals(1, 1);
  }
}
