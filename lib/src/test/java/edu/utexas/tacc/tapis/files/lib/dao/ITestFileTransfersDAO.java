package edu.utexas.tacc.tapis.files.lib.dao;

import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import org.mockito.*;
import org.testng.Assert;
import org.mockito.Mockito.*;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import org.flywaydb.core.Flyway;

import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;

@Test(groups = {"integration"})
public class ITestFileTransfersDAO {

//    Flyway flyway = Flyway.configure().dataSource("jdbc:postgresql://localhost:5432/test", "test", "test").load();
//
//    @BeforeTest
//    public void setUp() {
//        flyway.clean();
//        flyway.migrate();
//    }

    @Test
    public void testFileTransferInsertDAO() throws DAOException {
        FileTransfersDAO dao = new FileTransfersDAO();
        TransferTask task = new TransferTask();
        dao.createTransferTask(task);
    }

}