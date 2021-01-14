package edu.utexas.tacc.tapis.files.lib.dao;

import edu.utexas.tacc.tapis.files.lib.BaseDatabaseIntegrationTest;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskParent;
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
        TransferTaskParent task = new TransferTaskParent();
        task.setTenantId("test");
        task.setUsername("test");
        task.setSourceURI("tapis://test.tapis.io/test");
        task.setDestinationURI("tapis://test.tapis.io/test2");
        TransferTaskParent t = dao.createTransferTaskParent(task);
        Assert.assertTrue(t.getId() > 0);
    }

    @Test
    public void testGetTransfer() throws DAOException {
        FileTransfersDAO dao = new FileTransfersDAO();
        TransferTaskParent task = new TransferTaskParent();
        task.setTenantId("test");
        task.setUsername("test");
        task.setSourceURI("tapis://test.tapis.io/test");
        task.setDestinationURI("tapis://test.tapis.io/test2");
        TransferTaskParent t = dao.createTransferTaskParent(task);
        TransferTaskParent tNew = dao.getTransferTaskParentById(t.getId());
        Assert.assertEquals(tNew.getStatus(), TransferTaskStatus.ACCEPTED.name());
        Assert.assertNotNull(tNew.getCreated());
        Assert.assertEquals(t.getUuid(), tNew.getUuid());
    }

    @Test
    public void testGetAllForUser() throws DAOException {
        FileTransfersDAO dao = new FileTransfersDAO();
        TransferTaskParent task = new TransferTaskParent();
        task.setTenantId("test");
        task.setUsername("test");
        task.setSourceURI("tapis://test.tapis.io/test");
        task.setDestinationURI("tapis://test.tapis.io/test2");
        dao.createTransferTaskParent(task);
        dao.createTransferTaskParent(task);

        List<TransferTask> tasks = dao.getAllTransfersForUser(task.getTenantId(), task.getUsername());
        Assert.assertEquals(tasks.size(), 2);
    }

    @Test
    public void testUpdateChild() throws Exception {
        FileTransfersDAO dao = new FileTransfersDAO();
        TransferTaskParent task = new TransferTaskParent();
        task.setTenantId("test");
        task.setUsername("test");
        task.setSourceURI("tapis://test.tapis.io/test");
        task.setDestinationURI("tapis://test.tapis.io/test2");
        task = dao.createTransferTaskParent(task);

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