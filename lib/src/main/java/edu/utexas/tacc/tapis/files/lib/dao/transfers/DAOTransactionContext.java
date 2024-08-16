package edu.utexas.tacc.tapis.files.lib.dao.transfers;

import edu.utexas.tacc.tapis.files.lib.database.HikariConnectionPool;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

public class DAOTransactionContext implements AutoCloseable {
    private static Logger log = LoggerFactory.getLogger(DAOTransactionContext.class);
    private Connection connection = null;

    public static interface DAOOperation<T> {
        T doOperation(DAOTransactionContext context) throws DAOException;
    }

    public DAOTransactionContext() throws DAOException {
        this.connection = HikariConnectionPool.getConnection();
        try {
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            rollback();
            throw new DAOException(LibUtils.getMsg("FILES_TXFR_DAO_CONTEXT_ERROR", e.getMessage()), e);
        }
    }

    protected Connection getConnection() {
        return connection;
    }

    public void commit() throws DAOException {
        try {
            if (connectionIsOpen()) {
                connection.commit();
            }
        } catch (SQLException e) {
            throw new DAOException(LibUtils.getMsg("FILES_TXFR_DAO_CONTEXT_ERROR", e.getMessage()), e);
        }
    }

    public void rollback() throws DAOException {
        try {
            if (connectionIsOpen()) {
                connection.rollback();
            }
        } catch (SQLException e) {
            throw new DAOException(LibUtils.getMsg("FILES_TXFR_DAO_CONTEXT_ERROR", e.getMessage()), e);
        }
    }

    private boolean connectionIsOpen() throws DAOException {
        try {
            return ((connection != null) && (!connection.isClosed()));
        } catch (SQLException e) {
            throw new DAOException(LibUtils.getMsg("FILES_TXFR_DAO_CONTEXT_ERROR", e.getMessage()), e);
        }
    }

    @Override
    public void close() throws DAOException {
        try {
            if (connectionIsOpen()) {
                connection.close();
                connection = null;
            }
        } catch (SQLException e) {
            throw new DAOException(LibUtils.getMsg("FILES_TXFR_DAO_CONTEXT_ERROR", e.getMessage()), e);
        }
    }

    /**
     * Provide a lambda expression or use the functional interface DAOOperation&lt;T&gt; to provide the
     * code to do in the transaction.  If the code returns with no exception, the transaction will be
     * committed, if it throws an execption it's rolled back.  You can of course commit or rollback the
     * transaction in the code using context passed into the function.
     * @param op - function to do in a transaction
     * @return returns a value of type T
     * @param <T> return type
     * @throws DAOException - if an error occurs.
     */
    public static <T> T doInTransaction(DAOOperation<T> op) throws DAOException {
        DAOTransactionContext context = new DAOTransactionContext();
        try {
            T returnValue = op.doOperation(context);
            context.commit();
            return returnValue;
        } finally {
            // this will rollback anything not committed.  This really amounts to rolling back
            // if an exception occurs;
            context.rollback();
            context.close();
        }
    }

}
