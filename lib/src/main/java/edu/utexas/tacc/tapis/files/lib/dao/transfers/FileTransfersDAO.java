package edu.utexas.tacc.tapis.files.lib.dao.transfers;

import edu.utexas.tacc.tapis.files.lib.database.HikariConnectionPool;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import org.apache.commons.dbutils.*;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.UUID;
import org.slf4j.Logger;

import javax.validation.constraints.NotNull;

public class FileTransfersDAO implements IFileTransferDAO {

    private Logger log = LoggerFactory.getLogger(FileTransfersDAO.class);

    // TODO: There should be some way to not duplicate this code...
    private class TransferTaskRowProcessor extends BasicRowProcessor {

        @Override
        public Object toBean(ResultSet rs, Class type) throws SQLException {
            TransferTask task = new TransferTask();
            task.setId(rs.getInt("id"));
            task.setUsername(rs.getString("username"));
            task.setTenantId(rs.getString("tenant_id"));
            task.setSourceSystemId(rs.getString("source_system_id"));
            task.setSourcePath(rs.getString("source_path"));
            task.setDestinationSystemId(rs.getString("destination_system_id"));
            task.setDestinationPath(rs.getString("destination_path"));
            task.setCreated(rs.getTimestamp("created").toInstant());
            task.setUuid(UUID.fromString(rs.getString("uuid")));
            task.setStatus(TransferTaskStatus.valueOf(rs.getString("status")));
            task.setTotalBytes(rs.getLong("total_bytes"));
            task.setBytesTransferred(rs.getLong("bytes_transferred"));
            return task;
        }
    }

    private class TransferTaskChildRowProcessor extends BasicRowProcessor {

        @Override
        public Object toBean(ResultSet rs, Class type) throws SQLException {
            TransferTaskChild task = new TransferTaskChild();
            task.setId(rs.getInt("id"));
            task.setParentTaskId(rs.getInt("parent_task_id"));
            task.setUsername(rs.getString("username"));
            task.setTenantId(rs.getString("tenant_id"));
            task.setSourceSystemId(rs.getString("source_system_id"));
            task.setSourcePath(rs.getString("source_path"));
            task.setDestinationSystemId(rs.getString("destination_system_id"));
            task.setDestinationPath(rs.getString("destination_path"));
            task.setCreated(rs.getTimestamp("created").toInstant());
            task.setUuid(UUID.fromString(rs.getString("uuid")));
            task.setStatus(TransferTaskStatus.valueOf(rs.getString("status")));
            task.setTotalBytes(rs.getLong("total_bytes"));
            task.setBytesTransferred(rs.getLong("bytes_transferred"));
            return task;
        }
    }

    public TransferTask getTransferTask(@NotNull String uuid) throws DAOException {
        try {
            UUID taskUUID = UUID.fromString(uuid);
            return getTransferTask(taskUUID);
        } catch (IllegalArgumentException e) {
            throw new DAOException(0);
        }
    }

    public TransferTaskChild getTransferTaskChild(@NotNull String uuid) throws DAOException {
        try {
            UUID taskUUID = UUID.fromString(uuid);
            return getTransferTaskChild(taskUUID);
        } catch (IllegalArgumentException e) {
            throw new DAOException(0);
        }
    }

    public TransferTaskChild getTransferTaskChild(@NotNull  UUID taskUUID) throws DAOException {
        Connection connection = HikariConnectionPool.getConnection();
        RowProcessor rowProcessor = new TransferTaskChildRowProcessor();
        try {
            BeanHandler<TransferTaskChild> handler = new BeanHandler<>(TransferTaskChild.class, rowProcessor);
            String query = FileTransfersDAOStatements.GET_CHILD_TASK_BY_ID;
            QueryRunner runner = new QueryRunner();
            return runner.query(connection, query, handler, taskUUID);
        } catch (SQLException ex) {
            throw new DAOException(ex.getErrorCode());
        } finally {
            DbUtils.closeQuietly(connection);
        }
    }

    public TransferTask getTransferTask(@NotNull  UUID taskUUID) throws DAOException {
        Connection connection = HikariConnectionPool.getConnection();
        RowProcessor rowProcessor = new TransferTaskRowProcessor();
        try {
            BeanHandler<TransferTask> handler = new BeanHandler<>(TransferTask.class, rowProcessor);
            String query = FileTransfersDAOStatements.GET_PARENT_TASK_BY_ID;
            QueryRunner runner = new QueryRunner();
            TransferTask task = runner.query(connection, query, handler, taskUUID);
            return task;
        } catch (SQLException ex) {
            throw new DAOException(ex.getErrorCode());
        } finally {
            DbUtils.closeQuietly(connection);
        }
    }

    /**
     * This method is used to increment the total size of the transfer. As the directory
     * is recursively walked, we will add the size of the files to the current total.
     *
     * @param task
     * @param newBytes The size in bytes to be added to the total size of the transfer
     */
    public TransferTask updateTransferTaskSize(@NotNull TransferTask task, Long newBytes) throws DAOException {
        Connection connection = HikariConnectionPool.getConnection();
        RowProcessor rowProcessor = new TransferTaskRowProcessor();
        try {
            BeanHandler<TransferTask> handler = new BeanHandler<>(TransferTask.class, rowProcessor);
            String stmt = FileTransfersDAOStatements.UPDATE_PARENT_TASK_SIZE;
            QueryRunner runner = new QueryRunner();
            TransferTask updatedTask = runner.query(connection, stmt, handler,
                    newBytes,
                    task.getUuid());
            return updatedTask;
        } catch (SQLException ex) {
            throw new DAOException(ex.getErrorCode());
        } finally {
            DbUtils.closeQuietly(connection);
        }

    }


    public TransferTask updateTransferTask(@NotNull TransferTask task) throws DAOException {
        Connection connection = HikariConnectionPool.getConnection();
        RowProcessor rowProcessor = new TransferTaskRowProcessor();
        try {
            BeanHandler<TransferTask> handler = new BeanHandler<>(TransferTask.class, rowProcessor);
            String stmt = FileTransfersDAOStatements.UPDATE_PARENT_TASK;
            QueryRunner runner = new QueryRunner();
            TransferTask updatedTask = runner.query(connection, stmt, handler,
                    task.getSourceSystemId(),
                    task.getSourcePath(),
                    task.getDestinationSystemId(),
                    task.getDestinationPath(),
                    task.getStatus().name(),
                    task.getUuid());

            return updatedTask;
        } catch (SQLException ex) {
            throw new DAOException(ex.getErrorCode());
        } finally {
            DbUtils.closeQuietly(connection);
        }
    }

    public TransferTask createTransferTask(@NotNull TransferTask task) throws DAOException {

        task.setStatus(TransferTaskStatus.ACCEPTED);
        Connection connection = HikariConnectionPool.getConnection();
        RowProcessor rowProcessor = new TransferTaskRowProcessor();
        try {
            BeanHandler<TransferTask> handler = new BeanHandler<>(TransferTask.class, rowProcessor);
            String stmt = FileTransfersDAOStatements.INSERT_PARENT_TASK;
            QueryRunner runner = new QueryRunner();
            TransferTask insertedTask = runner.query(connection, stmt, handler,
                    task.getUuid(),
                    task.getTenantId(),
                    task.getUsername(),
                    task.getSourceSystemId(),
                    task.getSourcePath(),
                    task.getDestinationSystemId(),
                    task.getDestinationPath(),
                    task.getStatus().name());
            return insertedTask;
        } catch (SQLException ex) {
            throw new DAOException(ex.getErrorCode());
        } finally {
            DbUtils.closeQuietly(connection);
        }
    }

    public TransferTaskChild insertChildTask(@NotNull TransferTaskChild task) throws DAOException {
        Connection connection = HikariConnectionPool.getConnection();
        RowProcessor rowProcessor = new TransferTaskChildRowProcessor();

        try {
            BeanHandler<TransferTaskChild> handler = new BeanHandler<>(TransferTaskChild.class, rowProcessor);
            String stmt = FileTransfersDAOStatements.INSERT_CHILD_TASK;
            QueryRunner runner = new QueryRunner();
            TransferTaskChild child = runner.query(connection, stmt, handler,
              task.getTenantId(),
              task.getParentTaskId(),
              task.getUsername(),
              task.getSourceSystemId(),
              task.getSourcePath(),
              task.getDestinationSystemId(),
              task.getDestinationPath(),
              task.getStatus().name(),
              task.getBytesTransferred(),
              task.getTotalBytes()
            );

            return child;
        } catch (SQLException ex) {
            throw new DAOException(ex.getErrorCode());
        } finally {
            DbUtils.closeQuietly(connection);
        }
    }

}
