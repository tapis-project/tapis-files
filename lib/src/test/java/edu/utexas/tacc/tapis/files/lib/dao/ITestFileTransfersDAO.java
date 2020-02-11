package edu.utexas.tacc.tapis.files.lib.dao;

import edu.utexas.tacc.tapis.files.lib.BaseDatabaseIntegrationTest;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import org.flywaydb.core.api.Location;
import org.mockito.*;
import org.testng.Assert;
import org.mockito.Mockito.*;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import org.flywaydb.core.Flyway;

import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;

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
        Assert.assertEquals(t.getUuid(), task.getUuid());
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
        TransferTask tNew = dao.getTransferTask(t.getUuid());
        Assert.assertEquals(tNew.getUuid(), task.getUuid());
        Assert.assertEquals(tNew.getStatus(), "ACCEPTED");
        Assert.assertNotEquals(tNew.getCreated(), null);
    }
}