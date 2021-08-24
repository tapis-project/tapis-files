package edu.utexas.tacc.tapis.files.lib.dao;

import edu.utexas.tacc.tapis.files.lib.BaseDatabaseIntegrationTest;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskParent;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskRequestElement;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Test(groups={"integration"})
public class ITestFileTransfersDAO extends BaseDatabaseIntegrationTest {

    private final FileTransfersDAO dao = new FileTransfersDAO();

    private TransferTask createTransferTask() throws DAOException {
        TransferTask task = new TransferTask();
        task.setTag("testTag");
        task.setTenantId("testTenant");
        task.setUsername("testUser");
        task.setStatus(TransferTaskStatus.ACCEPTED.name());
        List<TransferTaskRequestElement> elements = new ArrayList<>();
        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setDestinationURI("tapis://sourceSystem/path");
        element.setSourceURI("tapis://destSystem/path");
        elements.add(element);

        TransferTaskRequestElement element2 = new TransferTaskRequestElement();
        element2.setDestinationURI("tapis://sourceSystem2/path");
        element2.setSourceURI("tapis://destSystem2/path");
        elements.add(element2);

        task = dao.createTransferTask(task, elements);
       return task;
    }


    @Test
    public void testCreateTransfer() throws DAOException {
        TransferTask task = createTransferTask();
        Assert.assertTrue(task.getId() > 0);
        Assert.assertEquals(task.getParentTasks().size(), 2);
        TransferTaskParent parent = task.getParentTasks().get(0);
        Assert.assertTrue(parent.getId() > 0);
        Assert.assertEquals(parent.getTaskId(), task.getId());
    }


    @Test
    public void testGetTaskByUUID() throws DAOException {
        TransferTask t = createTransferTask();
        TransferTask newTask = dao.getTransferTaskByUUID(t.getUuid());
        Assert.assertEquals(newTask.getId(), t.getId());
    }

    @Test
    public void testGetTransferParent() throws DAOException {
        TransferTask t = createTransferTask();
        TransferTaskParent parent = t.getParentTasks().get(0);
        TransferTaskParent newParent;
        newParent = dao.getTransferTaskParentByUUID(parent.getUuid());
        Assert.assertEquals(newParent.getUuid(), parent.getUuid());
    }

    @Test
    public void testGetAllParentsForTask() throws DAOException {

        TransferTask t = createTransferTask();
        List<TransferTaskParent> parents = dao.getAllParentsForTaskByID(t.getId());
        Assert.assertEquals(parents.size(), 2);
    }

    @Test
    public void testGetAllForUser() throws DAOException {
        TransferTask t1 = createTransferTask();
        TransferTask t2 = createTransferTask();

        List<TransferTask> tasks = dao.getRecentTransfersForUser("testTenant", "testUser", 1000, 0);
        Assert.assertEquals(tasks.size(), 2);
    }

    @Test
    public void testUpdateChild() throws Exception {
        TransferTask t1 = createTransferTask();
        TransferTaskParent parent = t1.getParentTasks().get(0);
        FileInfo fileInfo = new FileInfo();
        fileInfo.setPath("/a/b/c.txt");
        fileInfo.setSize(1000);
        fileInfo.setType("file");

        TransferTaskChild child = new TransferTaskChild(parent, fileInfo);
        child = dao.insertChildTask(child);

        child.setRetries(10);
        child.setBytesTransferred(10000);
        child.setStartTime(Instant.now());
        child.setEndTime(Instant.now());
        child = dao.updateTransferTaskChild(child);

        Assert.assertEquals(child.getRetries(), 10);
        Assert.assertEquals(child.getBytesTransferred(), 10000);
        Assert.assertNotNull(child.getStartTime());
        Assert.assertNotNull(child.getEndTime());
    }


    @Test
    public void testGetHistory() throws Exception {

        TransferTask t1 = createTransferTask();
        TransferTaskParent parent = t1.getParentTasks().get(0);
        FileInfo fileInfo = new FileInfo();
        fileInfo.setPath("/a/b/c.txt");
        fileInfo.setType("file");
        fileInfo.setSize(1000);

        //create 3 children on the first parent
        TransferTaskChild child = new TransferTaskChild(parent, fileInfo);
        dao.insertChildTask(child);
        dao.insertChildTask(child);
        dao.insertChildTask(child);

        TransferTask task = dao.getHistory(t1.getUuid());
        Assert.assertNotNull(task.getParentTasks());
        Assert.assertNotNull(task.getParentTasks().get(0).getChildren());
        Assert.assertEquals(task.getParentTasks().get(0).getChildren().size(), 3);

    }




}