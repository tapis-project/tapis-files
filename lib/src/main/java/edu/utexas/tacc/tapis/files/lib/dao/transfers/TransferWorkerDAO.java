package edu.utexas.tacc.tapis.files.lib.dao.transfers;

import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.transfers.TransferWorker;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.RowProcessor;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TransferWorkerDAO {
    public List<TransferWorker> getTransferWorkers(DAOTransactionContext context) throws DAOException {
        List<TransferWorker> retrievedWorkers = null;
        try {
            RowProcessor rowProcessor = new TransferWorkersRowProcessor();
            BeanListHandler<TransferWorker> handler = new BeanListHandler<>(TransferWorker.class, rowProcessor);
            QueryRunner runner = new QueryRunner();
            retrievedWorkers = runner.query(context.getConnection(), TransferWorkerDAOStatements.SELECT_ALL_TRANSFER_WORKERS, handler);
        } catch (SQLException e) {
            // TODO:  add to messsage catalog
            throw new DAOException("ERROR", e);
        }

        return retrievedWorkers;
    }

    public TransferWorker insertTransferWorker(DAOTransactionContext context) throws DAOException {
        TransferWorker transferWorker = null;
        try {
            RowProcessor rowProcessor = new TransferWorkersRowProcessor();
            BeanHandler<TransferWorker> handler = new BeanHandler<>(TransferWorker.class, rowProcessor);
            QueryRunner runner = new QueryRunner();
            transferWorker = runner.query(context.getConnection(), TransferWorkerDAOStatements.INSERT_TRANSFER_WORKER, handler);
        } catch (SQLException e) {
            // TODO:  add to messsage catalog
            throw new DAOException("ERROR", e);
        }

        return transferWorker;
    }

    public TransferWorker updateTransferWorker(DAOTransactionContext context, UUID uuid) throws DAOException {
        TransferWorker transferWorker = null;
        try {
            RowProcessor rowProcessor = new TransferWorkersRowProcessor();
            BeanHandler<TransferWorker> handler = new BeanHandler<>(TransferWorker.class, rowProcessor);
            QueryRunner runner = new QueryRunner();
            transferWorker = runner.query(context.getConnection(), TransferWorkerDAOStatements.UPDATE_TRANSFER_WORKER_TIME, handler, uuid);
            // TODO:  Add null check for trasnferWorker - means it couldn't be updated.  Maybe insert if this fails?  Not sure
        } catch (SQLException e) {
            // TODO:  add to messsage catalog
            throw new DAOException("ERROR", e);
        }

        return transferWorker;
    }

    public TransferWorker deleteTransferWorkerById(DAOTransactionContext context, UUID uuid) throws DAOException {
        TransferWorker transferWorker = null;
        try {
            RowProcessor rowProcessor = new TransferWorkersRowProcessor();
            BeanHandler<TransferWorker> handler = new BeanHandler<>(TransferWorker.class, rowProcessor);
            QueryRunner runner = new QueryRunner();
            transferWorker = runner.query(context.getConnection(), TransferWorkerDAOStatements.DELETE_TRANSFER_WORKER_BY_UUID, handler, uuid);
        } catch (SQLException e) {
            // TODO:  add to messsage catalog
            throw new DAOException("ERROR", e);
        }

        return transferWorker;
    }

    public TransferWorker getTransferWorkerById(DAOTransactionContext context, UUID uuid) throws DAOException {
        TransferWorker retrievedWorker = null;
        try {
            RowProcessor rowProcessor = new TransferWorkersRowProcessor();
            BeanHandler<TransferWorker> handler = new BeanHandler<>(TransferWorker.class, rowProcessor);
            QueryRunner runner = new QueryRunner();
            retrievedWorker = runner.query(context.getConnection(), TransferWorkerDAOStatements.SELECT_TRANSFER_WORKER_BY_UUID, handler, uuid);
        } catch (SQLException e) {
            // TODO:  add to messsage catalog
            throw new DAOException("ERROR", e);
        }

        return retrievedWorker;
    }

    public List<TransferWorker> getWorkersThatNeedWork() {
        List<TransferWorker> transferWorkersThatNeedWork = new ArrayList<>();



        return transferWorkersThatNeedWork;
    }
}
