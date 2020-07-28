package edu.utexas.tacc.tapis.files.lib.dao.transfers;

import edu.utexas.tacc.tapis.files.lib.database.HikariConnectionPool;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import org.apache.commons.dbutils.*;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;

import javax.validation.constraints.NotNull;

public class FileTransfersDAO implements IFileTransferDAO {

    private static final Logger log = LoggerFactory.getLogger(FileTransfersDAO.class);

    // TODO: There should be some way to not duplicate this code...
    private class TransferTaskRowProcessor extends BasicRowProcessor {

        @Override
        public TransferTask toBean(ResultSet rs, Class type) throws SQLException {
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
            task.setStatus(rs.getString("status"));
            task.setTotalBytes(rs.getLong("total_bytes"));
            task.setBytesTransferred(rs.getLong("bytes_transferred"));
            return task;
        }

        @Override
        public List<TransferTask> toBeanList(ResultSet rs, Class type) throws SQLException {
            List<TransferTask> list = new ArrayList<>();
            while (rs.next()) {
                list.add(toBean(rs, type));
            }
            return list;
        }
    }

    private class TransferTaskChildRowProcessor extends BasicRowProcessor {

        @Override
        public TransferTaskChild toBean(ResultSet rs, Class type) throws SQLException {
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
            task.setStatus(rs.getString("status"));
            task.setTotalBytes(rs.getLong("total_bytes"));
            task.setBytesTransferred(rs.getLong("bytes_transferred"));
            return task;
        }

        @Override
        public List<TransferTaskChild> toBeanList(ResultSet rs, Class type) throws SQLException {
            List<TransferTaskChild> list = new ArrayList<>();
            while (rs.next()) {
                list.add(toBean(rs, type));
            }
            return list;
        }
    }


    public TransferTaskChild getTransferTaskChild(@NotNull UUID taskUUID) throws DAOException {
        Connection connection = HikariConnectionPool.getConnection();
        RowProcessor rowProcessor = new TransferTaskChildRowProcessor();
        try {
            BeanHandler<TransferTaskChild> handler = new BeanHandler<>(TransferTaskChild.class, rowProcessor);
            String query = FileTransfersDAOStatements.GET_CHILD_TASK_BY_ID;
            QueryRunner runner = new QueryRunner();
            return runner.query(connection, query, handler, taskUUID);
        } catch (SQLException ex) {
            throw new DAOException(ex.getMessage(), ex);
        } finally {
            DbUtils.closeQuietly(connection);
        }
    }

    public TransferTask getTransferTaskByUUID(@NotNull UUID uuid) throws DAOException {
        Connection connection = HikariConnectionPool.getConnection();
        RowProcessor rowProcessor = new TransferTaskRowProcessor();
        try {
            BeanHandler<TransferTask> handler = new BeanHandler<>(TransferTask.class, rowProcessor);
            String query = FileTransfersDAOStatements.GET_PARENT_TASK_BY_UUID;
            QueryRunner runner = new QueryRunner();
            TransferTask task = runner.query(connection, query, handler, uuid);
            return task;
        } catch (SQLException ex) {
            throw new DAOException(ex.getMessage(), ex);
        } finally {
            DbUtils.closeQuietly(connection);
        }
    }

    public TransferTask getTransferTaskById(@NotNull long id) throws DAOException {
        Connection connection = HikariConnectionPool.getConnection();
        RowProcessor rowProcessor = new TransferTaskRowProcessor();
        try {
            BeanHandler<TransferTask> handler = new BeanHandler<>(TransferTask.class, rowProcessor);
            String query = FileTransfersDAOStatements.GET_PARENT_TASK_BY_ID;
            QueryRunner runner = new QueryRunner();
            TransferTask task = runner.query(connection, query, handler, id);
            return task;
        } catch (SQLException ex) {
            throw new DAOException(ex.getMessage(), ex);
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
                task.getId());
            return updatedTask;
        } catch (SQLException ex) {
            throw new DAOException(ex.getMessage(), ex);
        } finally {
            DbUtils.closeQuietly(connection);
        }
    }

    /**
     * This method is used to increment the bytes that have been transferred in the parent task
     *
     * @param taskId
     * @param newBytes The size in bytes to be added to the total size of the transfer
     */
    public TransferTask updateTransferTaskBytesTransferred(@NotNull long taskId, Long newBytes) throws DAOException {
        Connection connection = HikariConnectionPool.getConnection();
        RowProcessor rowProcessor = new TransferTaskRowProcessor();
        try {
            BeanHandler<TransferTask> handler = new BeanHandler<>(TransferTask.class, rowProcessor);
            String stmt = FileTransfersDAOStatements.UPDATE_PARENT_TASK_BYTES_TRANSFERRED;
            QueryRunner runner = new QueryRunner();
            TransferTask updatedTask = runner.query(connection, stmt, handler,
                newBytes,
                taskId);
            return updatedTask;
        } catch (SQLException ex) {
            throw new DAOException(ex.getMessage(), ex);
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
                task.getStatus(),
                task.getTotalBytes(),
                task.getUuid()
                );

            return updatedTask;
        } catch (SQLException ex) {
            throw new DAOException(ex.getMessage(), ex);
        } finally {
            DbUtils.closeQuietly(connection);
        }
    }

    public TransferTaskChild updateTransferTaskChild(@NotNull TransferTaskChild task) throws DAOException {
        Connection connection = HikariConnectionPool.getConnection();
        RowProcessor rowProcessor = new TransferTaskChildRowProcessor();
        try {
            BeanHandler<TransferTaskChild> handler = new BeanHandler<>(TransferTaskChild.class, rowProcessor);
            String stmt = FileTransfersDAOStatements.UPDATE_CHILD_TASK;
            QueryRunner runner = new QueryRunner();
            TransferTaskChild updatedTask = runner.query(connection, stmt, handler,
                task.getBytesTransferred(),
                task.getStatus(),
                task.getId()
            );
            return updatedTask;
        } catch (SQLException ex) {
            throw new DAOException(ex.getMessage(), ex);
        } finally {
            DbUtils.closeQuietly(connection);
        }
    }

    public TransferTask createTransferTask(@NotNull TransferTask task) throws DAOException {

        task.setStatus(TransferTaskStatus.ACCEPTED.name());
        Connection connection = HikariConnectionPool.getConnection();
        RowProcessor rowProcessor = new TransferTaskRowProcessor();
        try {
            BeanHandler<TransferTask> handler = new BeanHandler<>(TransferTask.class, rowProcessor);
            String stmt = FileTransfersDAOStatements.INSERT_PARENT_TASK;
            QueryRunner runner = new QueryRunner();
            TransferTask insertedTask = runner.query(connection, stmt, handler,
                task.getTenantId(),
                task.getUsername(),
                task.getSourceSystemId(),
                task.getSourcePath(),
                task.getDestinationSystemId(),
                task.getDestinationPath(),
                task.getStatus()
            );
            return insertedTask;
        } catch (SQLException ex) {
            throw new DAOException(ex.getMessage(), ex);
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
                task.getStatus(),
                task.getBytesTransferred(),
                task.getTotalBytes()
            );

            return child;
        } catch (SQLException ex) {
            log.error("ERROR", ex);
            throw new DAOException(ex.getMessage(), ex);
        } finally {
            DbUtils.closeQuietly(connection);
        }
    }

    public TransferTaskChild getChildTask(@NotNull TransferTaskChild task) throws DAOException {
        Connection connection = HikariConnectionPool.getConnection();
        RowProcessor rowProcessor = new TransferTaskChildRowProcessor();

        try {
            BeanHandler<TransferTaskChild> handler = new BeanHandler<>(TransferTaskChild.class, rowProcessor);
            String stmt = FileTransfersDAOStatements.GET_CHILD_TASK_BY_ID;
            QueryRunner runner = new QueryRunner();
            TransferTaskChild child = runner.query(connection, stmt, handler,
                task.getUuid()
            );

            return child;
        } catch (SQLException ex) {
            throw new DAOException(ex.getMessage(), ex);
        } finally {
            DbUtils.closeQuietly(connection);
        }
    }

    public List<TransferTask> getAllTransfersForUser(@NotNull String tenantId, @NotNull String username) throws DAOException {
        Connection connection = HikariConnectionPool.getConnection();
        RowProcessor rowProcessor = new TransferTaskRowProcessor();

        try {
            ResultSetHandler<List<TransferTask>> handler = new BeanListHandler<>(TransferTask.class, rowProcessor);
            String query = FileTransfersDAOStatements.GET_ALL_TASKS_FOR_USER;
            QueryRunner runner = new QueryRunner();
            List<TransferTask> tasks = runner.query(connection, query, handler,
                tenantId,
                username
            );
            return tasks;
        } catch (SQLException ex) {
            log.error("ERROR", ex);
            throw new DAOException(ex.getMessage(), ex);
        } finally {
            DbUtils.closeQuietly(connection);
        }
    }

    public long getIncompleteChildrenCount(@NotNull long taskId) throws DAOException {
        ScalarHandler<Long> scalarHandler = new ScalarHandler<>();
        Connection connection = HikariConnectionPool.getConnection();
        QueryRunner runner = new QueryRunner();
        String query = FileTransfersDAOStatements.GET_CHILD_TASK_INCOMPLETE_COUNT;
        try {
            long count = runner.query(connection, query, scalarHandler, taskId);
            return count;
        } catch (SQLException ex) {
            throw new DAOException(ex.getMessage(), ex);
        }
    }

    public List<TransferTaskChild> getAllChildren(@NotNull TransferTask task) throws DAOException {
        Connection connection = HikariConnectionPool.getConnection();
        RowProcessor rowProcessor = new TransferTaskChildRowProcessor();

        try {
            ResultSetHandler<List<TransferTaskChild>> handler = new BeanListHandler<>(TransferTaskChild.class, rowProcessor);
            String stmt = FileTransfersDAOStatements.GET_ALL_CHILDREN;
            QueryRunner runner = new QueryRunner();

            List<TransferTaskChild> children = runner.query(
                connection,
                stmt,
                handler,
                task.getId()
            );

            return children;
        } catch (SQLException ex) {
            log.error("ERROR", ex);
            throw new DAOException(ex.getMessage(), ex);
        } finally {
            DbUtils.closeQuietly(connection);
        }
    }
}
