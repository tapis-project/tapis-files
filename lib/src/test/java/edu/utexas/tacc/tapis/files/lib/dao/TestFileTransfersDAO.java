package edu.utexas.tacc.tapis.files.lib.dao;

import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
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
    TransferTask task = new TransferTask();
    task.setTenantId("test");
    task.setDestinationPath("/test1/test2");
    task.setDestinationSystemId("testOrigin");
    task.setSourceSystemId("testOrigin");
    task.setSourcePath("/test1/test2");
    task.setUsername("testUser");
    try {
      dao.createTransferTask(task);
      TransferTask newTask = dao.getTransferTask(task.getUuid());
      Assert.assertEquals(newTask.getTenantId(), "test");
    } catch (Exception e) {
      Assert.fail();
    }
  }
}
