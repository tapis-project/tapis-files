package edu.utexas.tacc.tapis.files.lib.dao.transfers;

import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.transfers.TransferWorker;
import org.flywaydb.core.Flyway;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Handler;

public class TestTransferWorkerDAO {

    TransferWorkerDAO dao;

    @BeforeMethod
    public void doFlywayMigrations()
    {
        Flyway flyway = Flyway.configure()
                .cleanDisabled(false)
                .dataSource("jdbc:postgresql://localhost:5432/test", "test", "test")
                .load();
        flyway.clean();
        flyway.migrate();
        dao = new TransferWorkerDAO();
    }

    @Test
    public void testCreateAndReadWorker() throws DAOException {
        TransferWorker insertedWorker = null;
        try(DAOTransactionContext context = new DAOTransactionContext()) {
            insertedWorker = dao.insertTransferWorker(context);
            context.commit();
        }
        checkWorkerFieldsNotNull(insertedWorker);
        TransferWorker retrievedWorker = null;
        try(DAOTransactionContext context = new DAOTransactionContext()) {
            retrievedWorker = dao.getTransferWorkerById(context, insertedWorker.getUuid());
        }
        compareWorkers(insertedWorker, retrievedWorker);
    }

    @Test
    public void testUpdateWorker() throws DAOException {
        TransferWorker insertedWorker = null;
        try(DAOTransactionContext context = new DAOTransactionContext()) {
            insertedWorker = dao.insertTransferWorker(context);
            context.commit();
        }
        checkWorkerFieldsNotNull(insertedWorker);
        TransferWorker updatedWorker = null;
        try(DAOTransactionContext context = new DAOTransactionContext()) {
            updatedWorker = dao.updateTransferWorker(context, insertedWorker.getUuid());
            context.commit();
        }
        Assert.assertTrue(updatedWorker.getLastUpdated().isAfter(insertedWorker.getLastUpdated()));
        TransferWorker retrievedWorker = null;
        try(DAOTransactionContext context = new DAOTransactionContext()) {
            retrievedWorker = dao.getTransferWorkerById(context, insertedWorker.getUuid());
        }
        compareWorkers(updatedWorker, retrievedWorker);
    }

    @Test
    public void testCreateAndListAllWorkers() throws DAOException {
        TransferWorker insertedWorker1 = null;
        TransferWorker insertedWorker2 = null;
        TransferWorker insertedWorker3 = null;

        try(DAOTransactionContext context = new DAOTransactionContext()) {
            insertedWorker1 = dao.insertTransferWorker(context);
            insertedWorker2 = dao.insertTransferWorker(context);
            insertedWorker3 = dao.insertTransferWorker(context);
            context.commit();
        }

        Map<UUID, TransferWorker> insertedWorkers = new HashMap<>();
        checkWorkerFieldsNotNull(insertedWorker1);
        checkWorkerFieldsNotNull(insertedWorker2);
        checkWorkerFieldsNotNull(insertedWorker3);

        insertedWorkers.put(insertedWorker1.getUuid(), insertedWorker1);
        insertedWorkers.put(insertedWorker2.getUuid(), insertedWorker2);
        insertedWorkers.put(insertedWorker3.getUuid(), insertedWorker3);


        List<TransferWorker> retrievedWorkers = null;
        try(DAOTransactionContext context = new DAOTransactionContext()) {
            retrievedWorkers = dao.getTransferWorkers(context);
        }

        for(TransferWorker worker : retrievedWorkers) {
            compareWorkers(worker, insertedWorkers.get(worker.getUuid()));
        }
    }

    @Test
    public void testDeleteWorkerById() throws DAOException {
        TransferWorker insertedWorker1 = null;
        TransferWorker insertedWorker2 = null;
        TransferWorker insertedWorker3 = null;

        try(DAOTransactionContext context = new DAOTransactionContext()) {
            insertedWorker1 = dao.insertTransferWorker(context);
            insertedWorker2 = dao.insertTransferWorker(context);
            insertedWorker3 = dao.insertTransferWorker(context);
            context.commit();
        }

        Map<UUID, TransferWorker> insertedWorkers = new HashMap<>();
        checkWorkerFieldsNotNull(insertedWorker1);
        checkWorkerFieldsNotNull(insertedWorker2);
        checkWorkerFieldsNotNull(insertedWorker3);

        insertedWorkers.put(insertedWorker1.getUuid(), insertedWorker1);
        insertedWorkers.put(insertedWorker2.getUuid(), insertedWorker2);
        insertedWorkers.put(insertedWorker3.getUuid(), insertedWorker3);


        int workersLeft = insertedWorkers.size();
        for(TransferWorker worker : insertedWorkers.values()) {
            try(DAOTransactionContext context = new DAOTransactionContext()) {
                compareWorkers(worker, dao.getTransferWorkerById(context, worker.getUuid()));
                dao.deleteTransferWorkerById(context, worker.getUuid());
                context.commit();
                Assert.assertNull(dao.getTransferWorkerById(context, worker.getUuid()));
                workersLeft--;
                List<TransferWorker> remainingWorkerList = dao.getTransferWorkers(context);
                Assert.assertEquals(workersLeft, remainingWorkerList.size());
            }
        }
        Assert.assertEquals(workersLeft, 0);
    }



    private void compareWorkers(TransferWorker worker1, TransferWorker worker2) {
        Assert.assertEquals(worker1.getUuid(), worker2.getUuid());
        Assert.assertEquals(worker1.getLastUpdated(), worker2.getLastUpdated());
    }

    private void checkWorkerFieldsNotNull(TransferWorker worker) {
        Assert.assertNotNull(worker.getUuid());
        Assert.assertNotNull(worker.getLastUpdated());
    }

}