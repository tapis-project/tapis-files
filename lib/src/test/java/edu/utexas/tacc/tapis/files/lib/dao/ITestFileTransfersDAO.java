package edu.utexas.tacc.tapis.files.lib.dao;

import edu.utexas.tacc.tapis.files.lib.BaseDatabaseIntegrationTest;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import org.flywaydb.core.api.Location;
import org.mockito.*;
import org.testng.Assert;
import org.mockito.Mockito.*;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import org.flywaydb.core.Flyway;

import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;

import java.util.List;

@Test(groups={"integration"})
public class ITestFileTransfersDAO extends BaseDatabaseIntegrationTest {


    @Test
    public void testCreateTransfer() throws DAOException {
        FileTransfersDAO dao = new FileTransfersDAO();
        TransferTask task = new TransferTask();
        task.setTenantId("test");
        task.setUsername("test");
        task.setDestinationSystemId("test");
        task.setDestinationPath("/test");
        task.setSourceSystemId("test2");
        task.setSourcePath("/test2");
        TransferTask t = dao.createTransferTask(task);
        Assert.assertTrue(t.getId() > 0);
    }

    @Test
    public void testGetTransfer() throws DAOException {
        FileTransfersDAO dao = new FileTransfersDAO();
        TransferTask task = new TransferTask();
        task.setTenantId("test");
        task.setUsername("test");
        task.setDestinationSystemId("test");
        task.setDestinationPath("/test");
        task.setSourceSystemId("test2");
        task.setSourcePath("/test2");
        TransferTask t = dao.createTransferTask(task);
        TransferTask tNew = dao.getTransferTaskById(t.getId());
        Assert.assertEquals(tNew.getStatus(), TransferTaskStatus.ACCEPTED.name());
        Assert.assertNotNull(tNew.getCreated());
    }

    @Test
    public void testGetAllForUser() throws DAOException {
        FileTransfersDAO dao = new FileTransfersDAO();
        TransferTask task = new TransferTask();
        task.setTenantId("test");
        task.setUsername("test");
        task.setDestinationSystemId("test");
        task.setDestinationPath("/test");
        task.setSourceSystemId("test2");
        task.setSourcePath("/test2");
        dao.createTransferTask(task);
        dao.createTransferTask(task);

        List<TransferTask> tasks = dao.getAllTransfersForUser(task.getTenantId(), task.getUsername());
        Assert.assertEquals(tasks.size(), 2);
    }

    @Test
    public void testUpdateChild() throws DAOException {
        FileTransfersDAO dao = new FileTransfersDAO();
        TransferTask task = new TransferTask();
        task.setTenantId("test");
        task.setUsername("test");
        task.setDestinationSystemId("test");
        task.setDestinationPath("/test");
        task.setSourceSystemId("test2");
        task.setSourcePath("/test2");
        task = dao.createTransferTask(task);

        FileInfo fileInfo = new FileInfo();
        fileInfo.setPath("/a/b/c.txt");
        fileInfo.setSize(1000);

        TransferTaskChild child = new TransferTaskChild(task, fileInfo);
        child = dao.insertChildTask(child);

        child.setRetries(10);
        child.setBytesTransferred(10000);
        child = dao.updateTransferTaskChild(child);

        Assert.assertEquals(child.getRetries(), 10);
        Assert.assertEquals(child.getBytesTransferred(), 10000);
    }




}