package edu.utexas.tacc.tapis.files.lib.dao.transfers;

import edu.utexas.tacc.tapis.files.lib.database.HikariConnectionPool;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskParent;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskRequestElement;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskSummary;
import org.apache.commons.dbutils.*;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;

import org.jetbrains.annotations.NotNull;

public class FileTransfersDAO {

    private static final Logger log = LoggerFactory.getLogger(FileTransfersDAO.class);


    private static class TransferTaskSummaryRowProcessor extends  BasicRowProcessor {
        @Override
        public TransferTaskSummary toBean(ResultSet rs, Class type) throws SQLException {
            TransferTaskSummary summary = new TransferTaskSummary();
            summary.setCompleteTransfers(rs.getInt("complete"));
            summary.setTotalTransfers(rs.getInt("total_transfers"));
            summary.setEstimatedTotalBytes(rs.getLong("total_bytes"));
            summary.setTotalBytesTransferred(rs.getLong("total_bytes_transferred"));
            return summary;
        }

        @Override
        public List<TransferTaskSummary> toBeanList(ResultSet rs, Class type) throws SQLException {
            List<TransferTaskSummary> list = new ArrayList<>();
            while (rs.next()) {
                list.add(toBean(rs, type));
            }
            return list;
        }
    }



    // TODO: There should be some way to not duplicate this code...
    private static class TransferTaskRowProcessor extends BasicRowProcessor {

        @Override
        public TransferTask toBean(ResultSet rs, Class type) throws SQLException {
            TransferTask task = new TransferTask();
            task.setId(rs.getInt("id"));
            task.setTenantId(rs.getString("tenant_id"));
            task.setUsername(rs.getString("username"));
            task.setCreated(rs.getTimestamp("created").toInstant());
            task.setUuid(UUID.fromString(rs.getString("uuid")));
            task.setStatus(rs.getString("status"));
            task.setTag(rs.getString("tag"));
            Optional.ofNullable(rs.getTimestamp("start_time")).ifPresent(ts-> task.setStartTime(ts.toInstant()));
            Optional.ofNullable(rs.getTimestamp("end_time")).ifPresent(ts-> task.setEndTime(ts.toInstant()));
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

    // TODO: There should be some way to not duplicate this code...
    private static class TransferTaskParentRowProcessor extends BasicRowProcessor {

        @Override
        public TransferTaskParent toBean(ResultSet rs, Class type) throws SQLException {
            TransferTaskParent task = new TransferTaskParent();
            task.setId(rs.getInt("id"));
            task.setTaskId(rs.getInt("task_id"));
            task.setUsername(rs.getString("username"));
            task.setTenantId(rs.getString("tenant_id"));
            task.setSourceURI(rs.getString("source_uri"));
            task.setDestinationURI(rs.getString("destination_uri"));
            task.setCreated(rs.getTimestamp("created").toInstant());
            task.setUuid(UUID.fromString(rs.getString("uuid")));
            task.setStatus(rs.getString("status"));
            task.setTotalBytes(rs.getLong("total_bytes"));
            task.setBytesTransferred(rs.getLong("bytes_transferred"));
            Optional.ofNullable(rs.getTimestamp("start_time")).ifPresent(ts-> task.setStartTime(ts.toInstant()));
            Optional.ofNullable(rs.getTimestamp("end_time")).ifPresent(ts-> task.setEndTime(ts.toInstant()));
            return task;
        }

        @Override
        public List<TransferTaskParent> toBeanList(ResultSet rs, Class type) throws SQLException {
            List<TransferTaskParent> list = new ArrayList<>();
            while (rs.next()) {
                list.add(toBean(rs, type));
            }
            return list;
        }
    }


    private static class TransferTaskChildRowProcessor extends BasicRowProcessor {

        @Override
        public TransferTaskChild toBean(ResultSet rs, Class type) throws SQLException {
            TransferTaskChild task = new TransferTaskChild();
            task.setId(rs.getInt("id"));
            task.setTaskId(rs.getInt("task_id"));
            task.setParentTaskId(rs.getInt("parent_task_id"));
            task.setUsername(rs.getString("username"));
            task.setTenantId(rs.getString("tenant_id"));
            task.setSourceURI(rs.getString("source_uri"));
            task.setDestinationURI(rs.getString("destination_uri"));
            task.setCreated(rs.getTimestamp("created").toInstant());
            task.setRetries(rs.getInt("retries"));
            task.setUuid(UUID.fromString(rs.getString("uuid")));
            task.setStatus(rs.getString("status"));
            task.setTotalBytes(rs.getLong("total_bytes"));
            task.setBytesTransferred(rs.getLong("bytes_transferred"));
            Optional.ofNullable(rs.getTimestamp("start_time")).ifPresent(ts-> task.setStartTime(ts.toInstant()));
            Optional.ofNullable(rs.getTimestamp("end_time")).ifPresent(ts-> task.setEndTime(ts.toInstant()));
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

    /**
     * Create a transfer task and all of the associated TransferTaskParent objects.
     * @param task
     * @param elements
     * @return Transfer task
     * @throws DAOException
     */
    public TransferTask createTransferTask(TransferTask task, List<TransferTaskRequestElement> elements) throws DAOException {
        RowProcessor rowProcessor = new TransferTaskRowProcessor();
        RowProcessor parentRowProcessor = new TransferTaskParentRowProcessor();
        BeanHandler<TransferTask> handler = new BeanHandler<>(TransferTask.class, rowProcessor);
        BeanHandler<TransferTaskParent> parentHandler = new BeanHandler<>(TransferTaskParent.class, parentRowProcessor);

        try (Connection connection = HikariConnectionPool.getConnection()) {
            String query = FileTransfersDAOStatements.INSERT_TASK;
            QueryRunner runner = new QueryRunner();
            TransferTask transferTask =  runner.query(connection, query, handler,
                task.getTenantId(),
                task.getUsername(),
                task.getStatus().name(),
                task.getTag()
            );

            // TODO: Bulk insert in one transaction
            List<TransferTaskParent> parentTasks = new ArrayList<>();
            for(TransferTaskRequestElement element: elements) {
                String insertParentTaskQuery = FileTransfersDAOStatements.INSERT_PARENT_TASK;
                TransferTaskParent parent = new TransferTaskParent();
                parent.setTenantId(transferTask.getTenantId());
                parent.setUsername(transferTask.getUsername());
                parent.setSourceURI(element.getSourceURI());
                parent.setDestinationURI(element.getDestinationURI());
                parent.setTaskId(transferTask.getId());
                parent.setStatus(TransferTaskStatus.ACCEPTED.name());
                parent =  runner.query(connection, insertParentTaskQuery, parentHandler,
                    parent.getTenantId(),
                    parent.getTaskId(),
                    parent.getUsername(),
                    parent.getSourceURI().toString(),
                    parent.getDestinationURI().toString(),
                    parent.getStatus().name()
                );
                parentTasks.add(parent);
            }
            transferTask.setParentTasks(parentTasks);
            return transferTask;
        } catch (SQLException ex) {
            throw new DAOException(ex.getMessage(), ex);
        }
    }

    public TransferTask getTransferTaskByUUID(@NotNull UUID taskUUID) throws DAOException {
        RowProcessor rowProcessor = new TransferTaskRowProcessor();
        RowProcessor summaryRowProcessor = new TransferTaskSummaryRowProcessor();
        try (Connection connection = HikariConnectionPool.getConnection()) {
            BeanHandler<TransferTask> handler = new BeanHandler<>(TransferTask.class, rowProcessor);
            String query = FileTransfersDAOStatements.GET_TASK_BY_UUID;
            QueryRunner runner = new QueryRunner();
            TransferTask task = runner.query(connection, query, handler, taskUUID);
            if (task ==null) {
                return null;
            }

            BeanHandler<TransferTaskSummary> summaryHandler = new BeanHandler<>(TransferTaskSummary.class, summaryRowProcessor);
            String summaryQuery = FileTransfersDAOStatements.GET_TRANSFER_TASK_SUMMARY_BY_UUID;
            QueryRunner summaryRunner = new QueryRunner();
            TransferTaskSummary summary = summaryRunner.query(connection, summaryQuery, summaryHandler, taskUUID);
            task.setTotalTransfers(summary.getTotalTransfers());
            task.setCompleteTransfers(summary.getCompleteTransfers());
            task.setTotalBytesTransferred(summary.getTotalBytesTransferred());
            task.setEstimatedTotalBytes(summary.getEstimatedTotalBytes());

            return task;
        } catch (SQLException ex) {
            throw new DAOException(ex.getMessage(), ex);
        }
    }

    public TransferTask getTransferTaskByID(@NotNull int taskId) throws DAOException {
        RowProcessor rowProcessor = new TransferTaskRowProcessor();
        RowProcessor summaryRowProcessor = new TransferTaskSummaryRowProcessor();
        try (Connection connection = HikariConnectionPool.getConnection()) {
            BeanHandler<TransferTask> handler = new BeanHandler<>(TransferTask.class, rowProcessor);
            String query = FileTransfersDAOStatements.GET_TASK_BY_ID;
            QueryRunner runner = new QueryRunner();
            TransferTask task = runner.query(connection, query, handler, taskId);
            if (task ==null) {
                return null;
            }

            BeanHandler<TransferTaskSummary> summaryHandler = new BeanHandler<>(TransferTaskSummary.class, summaryRowProcessor);
            String summaryQuery = FileTransfersDAOStatements.GET_TRANSFER_TASK_SUMMARY_BY_ID;
            QueryRunner summaryRunner = new QueryRunner();
            TransferTaskSummary summary = summaryRunner.query(connection, summaryQuery, summaryHandler, taskId);
            task.setTotalTransfers(summary.getTotalTransfers());
            task.setCompleteTransfers(summary.getCompleteTransfers());
            task.setTotalBytesTransferred(summary.getTotalBytesTransferred());
            task.setEstimatedTotalBytes(summary.getEstimatedTotalBytes());

            return task;
        } catch (SQLException ex) {
            throw new DAOException(ex.getMessage(), ex);
        }
    }

    public TransferTaskChild getTransferTaskChild(@NotNull UUID taskUUID) throws DAOException {
        RowProcessor rowProcessor = new TransferTaskChildRowProcessor();
        try (Connection connection = HikariConnectionPool.getConnection()) {
            BeanHandler<TransferTaskChild> handler = new BeanHandler<>(TransferTaskChild.class, rowProcessor);
            String query = FileTransfersDAOStatements.GET_CHILD_TASK_BY_UUID;
            QueryRunner runner = new QueryRunner();
            return runner.query(connection, query, handler, taskUUID);
        } catch (SQLException ex) {
            throw new DAOException(ex.getMessage(), ex);
        }
    }

    public List<TransferTaskParent> getAllParentsForTaskByID(@NotNull int taskId) throws DAOException {
        RowProcessor rowProcessor = new TransferTaskParentRowProcessor();
        try (Connection connection = HikariConnectionPool.getConnection()) {
            BeanListHandler<TransferTaskParent> handler = new BeanListHandler<>(TransferTaskParent.class, rowProcessor);
            String query = FileTransfersDAOStatements.GET_PARENTS_FOR_TASK_BY_ID;
            QueryRunner runner = new QueryRunner();
            List<TransferTaskParent> parentTasks = runner.query(connection, query, handler, taskId);
            return parentTasks;
        } catch (SQLException ex) {
            throw new DAOException(ex.getMessage(), ex);
        }
    }

    public TransferTaskParent getTransferTaskParentByUUID(@NotNull UUID uuid) throws DAOException {
        RowProcessor rowProcessor = new TransferTaskParentRowProcessor();
        try (Connection connection = HikariConnectionPool.getConnection()) {
            BeanHandler<TransferTaskParent> handler = new BeanHandler<>(TransferTaskParent.class, rowProcessor);
            String query = FileTransfersDAOStatements.GET_PARENT_TASK_BY_UUID;
            QueryRunner runner = new QueryRunner();
            TransferTaskParent task = runner.query(connection, query, handler, uuid);
            return task;
        } catch (SQLException ex) {
            throw new DAOException(ex.getMessage(), ex);
        }
    }

    public TransferTaskParent getTransferTaskParentById(@NotNull long id) throws DAOException {
        RowProcessor rowProcessor = new TransferTaskParentRowProcessor();
        try (Connection connection = HikariConnectionPool.getConnection()) {
            BeanHandler<TransferTaskParent> handler = new BeanHandler<>(TransferTaskParent.class, rowProcessor);
            String query = FileTransfersDAOStatements.GET_PARENT_TASK_BY_ID;
            QueryRunner runner = new QueryRunner();
            TransferTaskParent task = runner.query(connection, query, handler, id);
            return task;
        } catch (SQLException ex) {
            throw new DAOException(ex.getMessage(), ex);
        }
    }

    /**
     * This method is used to increment the total size of the transfer. As the directory
     * is recursively walked, we will add the size of the files to the current total.
     *
     * @param task
     */
    public TransferTask updateTransferTask(@NotNull TransferTask task) throws DAOException {
        RowProcessor rowProcessor = new TransferTaskRowProcessor();
        try (Connection connection = HikariConnectionPool.getConnection()) {
            BeanHandler<TransferTask> handler = new BeanHandler<>(TransferTask.class, rowProcessor);
            String stmt = FileTransfersDAOStatements.UPDATE_TRANSFER_TASK;
            QueryRunner runner = new QueryRunner();
            Timestamp startTime = null;
            Timestamp endTime = null;
            if (task.getStartTime() != null) {
                startTime = Timestamp.from(task.getStartTime());
            }
            if (task.getEndTime() != null) {
                endTime = Timestamp.from(task.getEndTime());
            }
            TransferTask updatedTask = runner.query(connection, stmt, handler,
                task.getStatus().name(),
                startTime,
                endTime,
                task.getId());
            return updatedTask;
        } catch (SQLException ex) {
            throw new DAOException(ex.getMessage(), ex);
        }
    }

    /**
     * This method is used to increment the total size of the transfer. As the directory
     * is recursively walked, we will add the size of the files to the current total.
     *
     * @param task
     * @param newBytes The size in bytes to be added to the total size of the transfer
     */
    public TransferTaskParent updateTransferTaskParentSize(@NotNull TransferTask task, Long newBytes) throws DAOException {
        RowProcessor rowProcessor = new TransferTaskParentRowProcessor();
        try (Connection connection = HikariConnectionPool.getConnection()) {
            BeanHandler<TransferTaskParent> handler = new BeanHandler<>(TransferTaskParent.class, rowProcessor);
            String stmt = FileTransfersDAOStatements.UPDATE_PARENT_TASK_SIZE;
            QueryRunner runner = new QueryRunner();
            TransferTaskParent updatedTask = runner.query(connection, stmt, handler,
                newBytes,
                task.getId());
            return updatedTask;
        } catch (SQLException ex) {
            throw new DAOException(ex.getMessage(), ex);
        }
    }

    /**
     * This method is used to increment the bytes that have been transferred in the parent task
     *
     * @param taskId
     * @param newBytes The size in bytes to be added to the total size of the transfer
     */
    public TransferTaskParent updateTransferTaskParentBytesTransferred(long taskId, Long newBytes) throws DAOException {
        RowProcessor rowProcessor = new TransferTaskParentRowProcessor();
        try (Connection connection = HikariConnectionPool.getConnection()) {
            BeanHandler<TransferTaskParent> handler = new BeanHandler<>(TransferTaskParent.class, rowProcessor);
            String stmt = FileTransfersDAOStatements.UPDATE_PARENT_TASK_BYTES_TRANSFERRED;
            QueryRunner runner = new QueryRunner();
            TransferTaskParent updatedTask = runner.query(connection, stmt, handler,
                newBytes,
                taskId);
            return updatedTask;
        } catch (SQLException ex) {
            throw new DAOException(ex.getMessage(), ex);
        }

    }

    public TransferTaskParent updateTransferTaskParent(@NotNull TransferTaskParent task) throws DAOException {
        RowProcessor rowProcessor = new TransferTaskParentRowProcessor();
        try (Connection connection = HikariConnectionPool.getConnection()) {
            BeanHandler<TransferTaskParent> handler = new BeanHandler<>(TransferTaskParent.class, rowProcessor);
            String stmt = FileTransfersDAOStatements.UPDATE_PARENT_TASK;
            QueryRunner runner = new QueryRunner();
            Timestamp startTime = null;
            Timestamp endTime = null;
            if (task.getStartTime() != null) {
                startTime = Timestamp.from(task.getStartTime());
            }
            if (task.getEndTime() != null) {
                endTime = Timestamp.from(task.getEndTime());
            }
            TransferTaskParent updatedTask = runner.query(connection, stmt, handler,
                task.getSourceURI().toString(),
                task.getDestinationURI().toString(),
                task.getStatus().name(),
                startTime,
                endTime,
                task.getBytesTransferred(),
                task.getTotalBytes(),
                task.getUuid()
            );

            return updatedTask;
        } catch (SQLException ex) {
            throw new DAOException(ex.getMessage(), ex);
        }
    }

    public TransferTaskChild updateTransferTaskChild(@NotNull TransferTaskChild task) throws DAOException {
        RowProcessor rowProcessor = new TransferTaskChildRowProcessor();
        try (Connection connection = HikariConnectionPool.getConnection()) {
            BeanHandler<TransferTaskChild> handler = new BeanHandler<>(TransferTaskChild.class, rowProcessor);
            String stmt = FileTransfersDAOStatements.UPDATE_CHILD_TASK;
            QueryRunner runner = new QueryRunner();
            Timestamp startTime = null;
            Timestamp endTime = null;
            if (task.getStartTime() != null) {
                startTime = Timestamp.from(task.getStartTime());
            }
            if (task.getEndTime() != null) {
                endTime = Timestamp.from(task.getEndTime());
            }

            TransferTaskChild updatedTask = runner.query(connection, stmt, handler,
                task.getBytesTransferred(),
                task.getStatus().name(),
                task.getRetries(),
                startTime,
                endTime,
                task.getId()
            );
            return updatedTask;
        } catch (SQLException ex) {
            throw new DAOException(ex.getMessage(), ex);
        }
    }

    public TransferTaskParent createTransferTaskParent(@NotNull TransferTaskParent task) throws DAOException {

        task.setStatus(TransferTaskStatus.ACCEPTED.name());
        RowProcessor rowProcessor = new TransferTaskParentRowProcessor();
        try (Connection connection = HikariConnectionPool.getConnection()) {
            BeanHandler<TransferTaskParent> handler = new BeanHandler<>(TransferTaskParent.class, rowProcessor);
            String stmt = FileTransfersDAOStatements.INSERT_PARENT_TASK;
            QueryRunner runner = new QueryRunner();
            TransferTaskParent insertedTask = runner.query(connection, stmt, handler,
                task.getTenantId(),
                task.getTaskId(),
                task.getUsername(),
                task.getSourceURI().toString(),
                task.getDestinationURI().toString(),
                task.getStatus().name()
                );
            return insertedTask;
        } catch (SQLException ex) {
            throw new DAOException(ex.getMessage(), ex);
        }
    }

    public void bulkInsertChildTasks(@NotNull List<TransferTaskChild> children) throws DAOException {
        List<Object[]> params = new ArrayList<>();
        children.forEach( (child)->{
            params.add( new Object[] {
                child.getTenantId(),
                child.getTaskId(),
                child.getParentTaskId(),
                child.getUsername(),
                child.getSourceURI().toString(),
                child.getDestinationURI().toString(),
                child.getStatus().name(),
                child.getBytesTransferred(),
                child.getTotalBytes()
            });
        });
        Object[][] t = new Object[params.size()][];
        params.toArray(t);
        try (Connection connection = HikariConnectionPool.getConnection()) {
            String stmt = FileTransfersDAOStatements.INSERT_CHILD_TASK;
            QueryRunner runner = new QueryRunner();
            runner.batch(connection, stmt, t);
        } catch (SQLException ex) {
            throw new DAOException("Bulk insert failed!", ex);
        }
    }

    public TransferTaskChild insertChildTask(@NotNull TransferTaskChild task) throws DAOException {
        RowProcessor rowProcessor = new TransferTaskChildRowProcessor();

        try (Connection connection = HikariConnectionPool.getConnection()) {
            BeanHandler<TransferTaskChild> handler = new BeanHandler<>(TransferTaskChild.class, rowProcessor);
            String stmt = FileTransfersDAOStatements.INSERT_CHILD_TASK;
            QueryRunner runner = new QueryRunner();
            TransferTaskChild child = runner.query(connection, stmt, handler,
                task.getTenantId(),
                task.getTaskId(),
                task.getParentTaskId(),
                task.getUsername(),
                task.getSourceURI().toString(),
                task.getDestinationURI().toString(),
                task.getStatus().name(),
                task.getBytesTransferred(),
                task.getTotalBytes()
            );

            return child;
        } catch (SQLException ex) {
            log.error("ERROR", ex);
            throw new DAOException(ex.getMessage(), ex);
        }
    }

    public TransferTaskChild getChildTaskByUUID(@NotNull TransferTaskChild task) throws DAOException {
        RowProcessor rowProcessor = new TransferTaskChildRowProcessor();

        try (Connection connection = HikariConnectionPool.getConnection()) {
            BeanHandler<TransferTaskChild> handler = new BeanHandler<>(TransferTaskChild.class, rowProcessor);
            String stmt = FileTransfersDAOStatements.GET_CHILD_TASK_BY_UUID;
            QueryRunner runner = new QueryRunner();
            TransferTaskChild child = runner.query(connection, stmt, handler,
                task.getUuid()
            );

            return child;
        } catch (SQLException ex) {
            throw new DAOException(ex.getMessage(), ex);
        }
    }

    public List<TransferTask> getRecentTransfersForUser(@NotNull String tenantId, @NotNull String username, int limit, int offset) throws DAOException {
        RowProcessor rowProcessor = new TransferTaskRowProcessor();

        try (Connection connection = HikariConnectionPool.getConnection()) {
            ResultSetHandler<List<TransferTask>> handler = new BeanListHandler<>(TransferTask.class, rowProcessor);
            String query = FileTransfersDAOStatements.GET_ALL_TASKS_FOR_USER;
            QueryRunner runner = new QueryRunner();
            List<TransferTask> tasks = runner.query(connection, query, handler,
                tenantId,
                username,
                limit,
                offset
            );
            return tasks;
        } catch (SQLException ex) {
            log.error("ERROR", ex);
            throw new DAOException(ex.getMessage(), ex);
        }
    }

    public long getIncompleteChildrenCount(@NotNull long taskId) throws DAOException {
        ScalarHandler<Long> scalarHandler = new ScalarHandler<>();
        QueryRunner runner = new QueryRunner();
        String query = FileTransfersDAOStatements.GET_CHILD_TASK_INCOMPLETE_COUNT;
        try (Connection connection = HikariConnectionPool.getConnection()) {
            long count = runner.query(connection, query, scalarHandler, taskId);
            return count;
        } catch (SQLException ex) {
            throw new DAOException(ex.getMessage(), ex);
        }
    }

    public long getIncompleteChildrenCountForParent(@NotNull long parentTaskId) throws DAOException {
        ScalarHandler<Long> scalarHandler = new ScalarHandler<>();
        QueryRunner runner = new QueryRunner();
        String query = FileTransfersDAOStatements.GET_CHILD_TASK_INCOMPLETE_COUNT_FOR_PARENT;
        try (Connection connection = HikariConnectionPool.getConnection()) {
            long count = runner.query(connection, query, scalarHandler, parentTaskId);
            return count;
        } catch (SQLException ex) {
            throw new DAOException(ex.getMessage(), ex);
        }
    }

    public List<TransferTaskChild> getAllChildren(@NotNull TransferTask task) throws DAOException {
        RowProcessor rowProcessor = new TransferTaskChildRowProcessor();

        try (Connection connection = HikariConnectionPool.getConnection()) {
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
        }
    }

    public List<TransferTaskChild> getAllChildren(@NotNull TransferTaskParent task) throws DAOException {
        RowProcessor rowProcessor = new TransferTaskChildRowProcessor();

        try (Connection connection = HikariConnectionPool.getConnection()) {
            ResultSetHandler<List<TransferTaskChild>> handler = new BeanListHandler<>(TransferTaskChild.class, rowProcessor);
            String stmt = FileTransfersDAOStatements.GET_ALL_CHILDREN_FOR_PARENT;
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
        }
    }

    public TransferTask getHistory(@NotNull UUID taskUUID) throws DAOException {
        //TODO: This could be done in one query with a couple of joins quicker
        TransferTask task = this.getTransferTaskByUUID(taskUUID);
        List<TransferTaskParent> parents = this.getAllParentsForTaskByID(task.getId());
        for (TransferTaskParent parent: parents) {
            List<TransferTaskChild> children = this.getAllChildren(parent);
            parent.setChildren(children);
        }
        task.setParentTasks(parents);
        return task;
    }

}
