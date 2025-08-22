package edu.utexas.tacc.tapis.files.lib.dao.transfers;

import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.models.ArchiveTransfer;
import edu.utexas.tacc.tapis.files.lib.models.PrioritizedObject;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.RowProcessor;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.ColumnListHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class ArchiveTransfersDAO {
    public static final Logger log = LoggerFactory.getLogger(ArchiveTransfersDAO.class);
    public ArchiveTransfer insertArchiveTransfer(DAOTransactionContext context, ArchiveTransfer archiveTransfer) throws DAOException {
        ArchiveTransfer insertedArchiveTransfer = null;
        try {
            RowProcessor rowProcessor = new ArchiveTransferRowProcessor();
            BeanHandler<ArchiveTransfer> handler = new BeanHandler<>(ArchiveTransfer.class, rowProcessor);
            QueryRunner runner = new QueryRunner();
            insertedArchiveTransfer = runner.query(context.getConnection(), ArchiveTransferDAOStatements.INSERT_ARCHIVE_TRANSFER, handler,
                    archiveTransfer.getUsername(),
                    archiveTransfer.getTenantId(),
                    archiveTransfer.getStatus().name(),
                    archiveTransfer.getSourceBaseUrl(),
                    archiveTransfer.getDestinationBaseUrl(),
                    archiveTransfer.getArchiveType(),
                    archiveTransfer.getArchiveBytesRead(),
                    archiveTransfer.getFileBytesRead(),
                    archiveTransfer.getErrorMessage(),
                    archiveTransfer.getSrcSharedCtxGrantor(),
                    archiveTransfer.getDestSharedCtxGrantor());
            insertedArchiveTransfer.setRelativePaths(insertRelativePaths(context, insertedArchiveTransfer.getId(), archiveTransfer.getRelativePaths()));
        } catch (SQLException ex) {
            throw new DAOException(LibUtils.getMsg("FILES_TXFR_DAO_ERR_GENERAL", "insertTransferWorker", ex.getMessage()), ex);
        }

        return insertedArchiveTransfer;
    }
    public Set<String> insertRelativePaths(DAOTransactionContext context, int archiveTransferId, Set<String> relativePaths) throws DAOException {
        Set<String> insertedPaths = new HashSet<>();
        try {
            ResultSetHandler<String> handler = new ScalarHandler<>("path");
            QueryRunner runner = new QueryRunner();
            for(String relativePath : relativePaths) {
                String insertedPath = runner.query(context.getConnection(),
                        ArchiveTransferDAOStatements.INSERT_RELATIVE_PATHS, handler, archiveTransferId, relativePath);
                insertedPaths.add(insertedPath);
            }
        } catch (SQLException ex) {
            throw new DAOException(LibUtils.getMsg("FILES_TXFR_DAO_ERR_GENERAL", "insertTransferWorker", ex.getMessage()), ex);
        }

        return insertedPaths;
    }

    public Set<String> getRelativePaths(DAOTransactionContext context, int archiveTransferId) throws DAOException {
        Set<String> insertedPaths = Collections.emptySet();
        try {
            ResultSetHandler<List<String>> handler = new ColumnListHandler<String>("path");

            QueryRunner runner = new QueryRunner();
            List<String> paths = runner.query(context.getConnection(), ArchiveTransferDAOStatements.GET_RELATIVE_PATHS_FOR_ID, handler, archiveTransferId);
            if(paths != null) {
                insertedPaths = new HashSet<>(paths);
            }
        } catch (SQLException ex) {
            throw new DAOException(LibUtils.getMsg("FILES_TXFR_DAO_ERR_GENERAL", "insertTransferWorker", ex.getMessage()), ex);
        }

        return insertedPaths;
    }

    public ArchiveTransfer getArchiveTransfer(DAOTransactionContext context, UUID archiveTransferUuid, boolean forUpdate, boolean includeRelativePaths) throws DAOException {
        RowProcessor rowProcessor = new ArchiveTransferRowProcessor();
        BeanHandler<ArchiveTransfer> handler = new BeanHandler<>(ArchiveTransfer.class, rowProcessor);

        try {
            String sqlString = forUpdate ?
                    ArchiveTransferDAOStatements.GET_ARCHIVE_TRANSFER_FOR_UPDATE :
                    ArchiveTransferDAOStatements.GET_ARCHIVE_TRANSFER;
            QueryRunner runner = new QueryRunner();
            ArchiveTransfer archiveTransfer = runner.query(
                    context.getConnection(),
                    sqlString,
                    handler,
                    archiveTransferUuid);

            if((archiveTransfer != null) && (includeRelativePaths)) {
                archiveTransfer.setRelativePaths(getRelativePaths(context, archiveTransfer.getId()));
            }

            return archiveTransfer;
        } catch (SQLException ex) {
            throw new DAOException(LibUtils.getMsg("FILES_TXFR_DAO_ERR_GENERAL", "getAssignedForWorker", ex.getMessage()), ex);
        }
    }

    public ArchiveTransfer updateArchiveTransfer(DAOTransactionContext context, ArchiveTransfer archiveTransfer, boolean includeRelativePaths) throws DAOException {
        RowProcessor rowProcessor = new ArchiveTransferRowProcessor();
        BeanHandler<ArchiveTransfer> handler = new BeanHandler<>(ArchiveTransfer.class, rowProcessor);

        try {
            QueryRunner runner = new QueryRunner();
            ArchiveTransfer updatedArchiveTransfer = runner.query(
                    context.getConnection(),
                    ArchiveTransferDAOStatements.UPDATE_ARCHIVE_TRANSFER,
                    handler,
                    archiveTransfer.getStatus().name(),
                    archiveTransfer.getErrorMessage(),
                    archiveTransfer.getArchiveBytesRead(),
                    archiveTransfer.getFileBytesRead(),
                    (archiveTransfer.getStartTime() == null) ? null : Timestamp.from(archiveTransfer.getStartTime()),
                    (archiveTransfer.getEndTime() == null) ? null : Timestamp.from(archiveTransfer.getEndTime()),
                    archiveTransfer.getAssignedTo(),
                    archiveTransfer.getUuid()
                    );

            if(includeRelativePaths) {
                updatedArchiveTransfer.setRelativePaths(getRelativePaths(context, archiveTransfer.getId()));
            }

            return updatedArchiveTransfer;
        } catch (SQLException ex) {
            throw new DAOException(LibUtils.getMsg("FILES_TXFR_DAO_ERR_GENERAL", "getAssignedForWorker", ex.getMessage()), ex);
        }
    }


    public List<PrioritizedObject<ArchiveTransfer>> getAssignedWorkForWorker(DAOTransactionContext context, int maxTasksPerTenantAndUser, UUID workerUuid) throws DAOException {
        RowProcessor rowProcessor = new PrioritizedObjectRowProcessor(new ArchiveTransferRowProcessor(), ArchiveTransfer.class);

        try {
            ResultSetHandler<List<PrioritizedObject>> handler =
                    new BeanListHandler<PrioritizedObject>(PrioritizedObject.class, rowProcessor);

            QueryRunner runner = new QueryRunner();
            List<PrioritizedObject> prioritizedArchiveTransfers;
            prioritizedArchiveTransfers = runner.query(
                    context.getConnection(),
                    ArchiveTransferDAOStatements.GET_ACCEPTED_ARCHIVE_TRANSFERS_ASSIGNED_TO_WORKER,
                    handler,
                    workerUuid,
                    maxTasksPerTenantAndUser);

            for(PrioritizedObject<ArchiveTransfer> prioritizedObject : prioritizedArchiveTransfers) {
                ArchiveTransfer archiveTransfer = prioritizedObject.getObject();
                archiveTransfer.setRelativePaths(getRelativePaths(context, archiveTransfer.getId()));
            }

            return (List<PrioritizedObject<ArchiveTransfer>>)(Object)prioritizedArchiveTransfers;
        } catch (SQLException ex) {
            throw new DAOException(LibUtils.getMsg("FILES_TXFR_DAO_ERR_GENERAL", "getAssignedForWorker", ex.getMessage()), ex);
        }
    }

    public List<PrioritizedObject<ArchiveTransfer>> getAcceptedArchiveTransfersForTenantsAndUsers(DAOTransactionContext context, int maxTasksPerTenantAndUser) throws DAOException {
        RowProcessor rowProcessor = new PrioritizedObjectRowProcessor(new ArchiveTransferRowProcessor(), ArchiveTransfer.class);

        try {
            ResultSetHandler<List<PrioritizedObject>> handler =
                    new BeanListHandler<PrioritizedObject>(PrioritizedObject.class, rowProcessor);

            QueryRunner runner = new QueryRunner();
            List<PrioritizedObject> prioritizedArchiveTransfers = runner.query(
                    context.getConnection(),
                    ArchiveTransferDAOStatements.GET_ACCEPTED_ARCHIVE_TRANSFERS_FOR_TENANTS_AND_USERS,
                    handler,
                    maxTasksPerTenantAndUser);

            for(PrioritizedObject<ArchiveTransfer> prioritizedObject : prioritizedArchiveTransfers) {
                ArchiveTransfer archiveTransfer = prioritizedObject.getObject();
                archiveTransfer.setRelativePaths(getRelativePaths(context, archiveTransfer.getId()));
            }

            return (List<PrioritizedObject<ArchiveTransfer>>)(Object)prioritizedArchiveTransfers;
        } catch (SQLException ex) {
            throw new DAOException(LibUtils.getMsg("FILES_TXFR_DAO_ERR_GENERAL", "getAcceptedArchiveTransfersForTenantsAndUsers", ex.getMessage()), ex);
        }
    }

    public Map<UUID, Integer> getAssignedWorkerCount(DAOTransactionContext context) throws DAOException {
        Map<UUID, Integer> assignedWorkerCount = new HashMap<>();
        try {
            String sql = ArchiveTransferDAOStatements.GET_ASSIGNED_ARCHIVE_TRANSFER_COUNT;
            PreparedStatement stmt = context.getConnection().prepareStatement(sql);
            ResultSet result = stmt.executeQuery();
            while(result.next()) {
                int count = result.getInt("count");
                UUID uuid = result.getObject("assigned_to", UUID.class);
                assignedWorkerCount.put(uuid, Integer.valueOf(count));
            }
            return assignedWorkerCount;
        } catch (SQLException ex) {
            throw new DAOException(LibUtils.getMsg("FILES_TXFR_DAO_ERR_GENERAL", "getAssignedWorkerCount", ex.getMessage()), ex);
        }
    }

    public void assignToWorkers(DAOTransactionContext context, List<Integer> taskIds, UUID workerId) throws DAOException {
        if(workerId == null) {
            log.error(LibUtils.getMsg("FILES_TXFR_DAO_NO_WORKER_PROVIDED", "assignToWorkers"));
            return;
        }

        if(taskIds.isEmpty()) {
            log.debug(LibUtils.getMsg("FILES_TXFR_DAO_NO_TASKS_TO_ASSIGN", "assignToWorkers"));
            return;
        }

        try {
            QueryRunner runner = new QueryRunner();
            final Array tasksToUpdate = context.getConnection().createArrayOf("long", taskIds.toArray());
            int numberAssigned = runner.update(context.getConnection(), ArchiveTransferDAOStatements.ASSIGN_TASKS_TO_WORKER, workerId, tasksToUpdate);
            log.info(LibUtils.getMsg("FILES_TXFR_DAO_ASSIGNED", numberAssigned, taskIds.size(), workerId));
        } catch (SQLException ex) {
            throw new DAOException(LibUtils.getMsg("FILES_TXFR_DAO_ERR_GENERAL", "assignToWorkers", ex.getMessage()), ex);
        }
    }

    public void cleanupZombieArchiveTransferAssignments(DAOTransactionContext context, Set<TransferTaskStatus> terminalStates) throws DAOException {
        try {
            // first unassign everything that's in a non-terminal state
            QueryRunner runner = new QueryRunner();
            final Array finalStates = context.getConnection().createArrayOf("text", terminalStates.stream().map(value -> value.name()).toArray());
            int zombies = runner.update(context.getConnection(), ArchiveTransferDAOStatements.UNASSIGN_ZOMBIE_ASSIGNMENTS, finalStates);
            if(zombies > 0) {
                log.info(LibUtils.getMsg("FILES_TXFR_DAO_REASSIGNED_ZOMBIES", zombies, "archiveTransfers"));
            }
        } catch (SQLException ex) {
            throw new DAOException(LibUtils.getMsg("FILES_TXFR_DAO_ERR_GENERAL", "cleanupZombieParentAssignments", ex.getMessage()), ex);
        }
    }

}
