package edu.utexas.tacc.tapis.files.lib.dao.transfers;

import edu.utexas.tacc.tapis.files.lib.database.HikariConnectionPool;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;

import java.sql.Connection;
import java.sql.SQLException;

public class DAOTransactionContext implements AutoCloseable {
    private Connection connection = null;

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

}
