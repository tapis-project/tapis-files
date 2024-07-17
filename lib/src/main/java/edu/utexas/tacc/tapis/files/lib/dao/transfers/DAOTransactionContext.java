package edu.utexas.tacc.tapis.files.lib.dao.transfers;

import edu.utexas.tacc.tapis.files.lib.database.HikariConnectionPool;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
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
            // TODO:  update message
            throw new DAOException("ERROR", e);
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
            // TODO:  update message
            throw new DAOException("ERROR", e);
        }
    }

    public void rollback() throws DAOException {
        try {
            if (connectionIsOpen()) {
                connection.rollback();
            }
        } catch (SQLException e) {
            // TODO:  update message
            throw new DAOException("ERROR", e);
        }
    }

    private boolean connectionIsOpen() throws DAOException {
        try {
            return ((connection != null) && (!connection.isClosed()));
        } catch (SQLException e) {
            // TODO:  update message
            throw new DAOException("ERROR", e);
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
            // TODO:  update message
            throw new DAOException("ERROR", e);
        }
    }

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
