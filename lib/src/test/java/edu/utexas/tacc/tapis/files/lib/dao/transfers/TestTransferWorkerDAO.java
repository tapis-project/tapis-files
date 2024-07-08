package edu.utexas.tacc.tapis.files.lib.dao.transfers;

import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.transfers.TransferWorker;
import org.flywaydb.core.Flyway;
import org.jooq.DAO;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

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
        TransferWorker insertedWorker = DAOTransactionContext.doInTransaction((context) -> {
            TransferWorker worker = dao.insertTransferWorker(context);
            context.commit();
            return worker;
        });

        checkWorkerFieldsNotNull(insertedWorker);
        TransferWorker retrievedWorker = DAOTransactionContext.doInTransaction((context) -> {
            return dao.getTransferWorkerById(context, insertedWorker.getUuid());
        });

        compareWorkers(insertedWorker, retrievedWorker);
    }

    @Test
    public void testUpdateWorker() throws DAOException {
        TransferWorker insertedWorker = DAOTransactionContext.doInTransaction((context) -> {
            TransferWorker worker = dao.insertTransferWorker(context);
            context.commit();
            return worker;
        });
        checkWorkerFieldsNotNull(insertedWorker);
        TransferWorker updatedWorker = DAOTransactionContext.doInTransaction((context) -> {
            TransferWorker worker = dao.updateTransferWorker(context, insertedWorker.getUuid());
            context.commit();
            return worker;
        });
        Assert.assertTrue(updatedWorker.getLastUpdated().isAfter(insertedWorker.getLastUpdated()));
        TransferWorker retrievedWorker = DAOTransactionContext.doInTransaction((context) -> {
            return dao.getTransferWorkerById(context, insertedWorker.getUuid());
        });
        compareWorkers(updatedWorker, retrievedWorker);
    }

    @Test
    public void testCreateAndListAllWorkers() throws DAOException {
        List<TransferWorker> insertedWorkers = new ArrayList<>();

        DAOTransactionContext.doInTransaction((context) -> {
            insertedWorkers.add(dao.insertTransferWorker(context));
            insertedWorkers.add(dao.insertTransferWorker(context));
            insertedWorkers.add(dao.insertTransferWorker(context));
            context.commit();
            return insertedWorkers;
        });

        Map<UUID, TransferWorker> insertedWorkerMap = new HashMap<>();
        for(TransferWorker worker : insertedWorkers) {
            checkWorkerFieldsNotNull(worker);
            insertedWorkerMap.put(worker.getUuid(), worker);
        }

        List<TransferWorker> retrievedWorkers = DAOTransactionContext.doInTransaction((context -> {
            return dao.getTransferWorkers(context);
        }));

        for(TransferWorker worker : retrievedWorkers) {
            compareWorkers(worker, insertedWorkerMap.get(worker.getUuid()));
        }
    }

    @Test
    public void testDeleteWorkerById() throws DAOException {
        Map<UUID, TransferWorker> insertedWorkers = new HashMap<>();

        int workersToCreate = 3;
        DAOTransactionContext.doInTransaction((context) -> {
            for(int i = 0;i < workersToCreate;i++) {
                TransferWorker worker = dao.insertTransferWorker(context);
                checkWorkerFieldsNotNull(worker);
                insertedWorkers.put(worker.getUuid(), worker);
            }
            context.commit();

            Assert.assertEquals(insertedWorkers.size(), workersToCreate);
            return insertedWorkers;
        });

        Iterator<TransferWorker> workerIterator= insertedWorkers.values().iterator();
        while(workerIterator.hasNext()) {
            TransferWorker worker = workerIterator.next();
            DAOTransactionContext.doInTransaction((context) -> {
                compareWorkers(worker, dao.getTransferWorkerById(context, worker.getUuid()));
                dao.deleteTransferWorkerById(context, worker.getUuid());
                context.commit();
                Assert.assertNull(dao.getTransferWorkerById(context, worker.getUuid()));
                List<TransferWorker> remainingWorkerList = dao.getTransferWorkers(context);
                workerIterator.remove();
                Assert.assertEquals(remainingWorkerList.size(), insertedWorkers.size());
                return null;
            });
        }
        Assert.assertEquals(insertedWorkers.size(), 0);
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