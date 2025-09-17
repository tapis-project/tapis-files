package edu.utexas.tacc.tapis.files.lib.dao.transfers;

import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.models.ArchiveTransfer;
import edu.utexas.tacc.tapis.files.lib.models.ArchiveTransferLogEntry;
import edu.utexas.tacc.tapis.files.lib.models.ArchiveTransferStatus;
import edu.utexas.tacc.tapis.files.lib.models.PrioritizedObject;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import org.apache.commons.dbutils.ColumnHandler;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.RowProcessor;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.ColumnListHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collection;
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
                    archiveTransfer.getDestSharedCtxGrantor(),
                    archiveTransfer.getRetriesRemaining());
            insertedArchiveTransfer.setRelativePaths(insertRelativePaths(context, insertedArchiveTransfer.getId(), archiveTransfer.getRelativePaths()));
            insertedArchiveTransfer.setTransferLogEntries(insertTransferLogEntries(context, insertedArchiveTransfer.getId(), archiveTransfer.getTransferLogEntries()));
        } catch (SQLException ex) {
            throw new DAOException(LibUtils.getMsg("FILES_TXFR_DAO_ERR_GENERAL", "insertTransferWorker", ex.getMessage()), ex);
        }

        return insertedArchiveTransfer;
    }
    public Set<String> insertRelativePaths(DAOTransactionContext context, int archiveTransferId, Set<String> relativePaths) throws DAOException {
        if((relativePaths == null) || (relativePaths.isEmpty())) {
            return Collections.emptySet();
        }
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
            throw new DAOException(LibUtils.getMsg("FILES_TXFR_DAO_ERR_GENERAL", "insertRelativePaths", ex.getMessage()), ex);
        }

        return insertedPaths;
    }

    public Set<String> getRelativePaths(DAOTransactionContext context, int archiveTransferId) throws DAOException {
        Set<String> relativePaths = Collections.emptySet();
        try {
            ResultSetHandler<List<String>> handler = new ColumnListHandler<String>("path");

            QueryRunner runner = new QueryRunner();
            List<String> paths = runner.query(context.getConnection(), ArchiveTransferDAOStatements.GET_RELATIVE_PATHS_FOR_ID, handler, archiveTransferId);
            if(paths != null) {
                relativePaths = new HashSet<>(paths);
            }
        } catch (SQLException ex) {
            throw new DAOException(LibUtils.getMsg("FILES_TXFR_DAO_ERR_GENERAL", "getRelativePaths", ex.getMessage()), ex);
        }

        return relativePaths;
    }

    public List<ArchiveTransferLogEntry> insertTransferLogEntries(DAOTransactionContext context, int archiveTransferId, List<ArchiveTransferLogEntry> transferLogEntries) throws DAOException {
        PGobject insertedTransferLogEntries;
        try {
            ScalarHandler<PGobject> handler = new ScalarHandler<>("log_entries");

            QueryRunner runner = new QueryRunner();
            PGobject logObject = new PGobject();
            logObject.setValue((transferLogEntries) == null ? null : TapisGsonUtils.getGson().toJson(transferLogEntries));
            logObject.setType("jsonb");
            insertedTransferLogEntries= runner.query(context.getConnection(), ArchiveTransferDAOStatements.INSERT_TRANSFER_LOG, handler, archiveTransferId, logObject);
        } catch (SQLException ex) {
            throw new DAOException(LibUtils.getMsg("FILES_TXFR_DAO_ERR_GENERAL", "insertArchiveTransferLog", ex.getMessage()), ex);
        }

        return (insertedTransferLogEntries == null) ? Collections.emptyList() :
        TapisGsonUtils.getGson().fromJson(insertedTransferLogEntries.getValue(), new TypeToken<List<ArchiveTransferLogEntry>>() {});
    }

    public List<ArchiveTransferLogEntry> updateTransferLogEntries(DAOTransactionContext context, int archiveTransferId,
                                                                  List<ArchiveTransferLogEntry> transferLogEntries) throws DAOException {
        PGobject insertedTransferLogEntries;
        try {
            ScalarHandler<PGobject> handler = new ScalarHandler<>("log_entries");

            QueryRunner runner = new QueryRunner();
            PGobject logObject = new PGobject();
            logObject.setValue((transferLogEntries) == null ? null : TapisGsonUtils.getGson().toJson(transferLogEntries));
            logObject.setType("jsonb");
            insertedTransferLogEntries= runner.query(context.getConnection(), ArchiveTransferDAOStatements.UPDATE_TRANSFER_LOG, handler, logObject, archiveTransferId);
        } catch (SQLException ex) {
            throw new DAOException(LibUtils.getMsg("FILES_TXFR_DAO_ERR_GENERAL", "updateArchiveTransferLog", ex.getMessage()), ex);
        }

        return (insertedTransferLogEntries == null) ? Collections.emptyList() :
                TapisGsonUtils.getGson().fromJson(insertedTransferLogEntries.getValue(), new TypeToken<List<ArchiveTransferLogEntry>>() {});
    }

    public List<ArchiveTransferLogEntry> getTransferLogEntries(DAOTransactionContext context, int archiveTransferId) throws DAOException {
        PGobject transferLogEntries;
        try {
            ScalarHandler<PGobject> handler = new ScalarHandler<>("log_entries");

            QueryRunner runner = new QueryRunner();
            transferLogEntries = runner.query(context.getConnection(), ArchiveTransferDAOStatements.GET_ARCHIVE_LOG_FOR_ID, handler, archiveTransferId);
        } catch (SQLException ex) {
            throw new DAOException(LibUtils.getMsg("FILES_TXFR_DAO_ERR_GENERAL", "getArchiveTransferLog", ex.getMessage()), ex);
        }

        return (transferLogEntries == null) ? Collections.emptyList() :
                TapisGsonUtils.getGson().fromJson(transferLogEntries.getValue(), new TypeToken<List<ArchiveTransferLogEntry>>() {});
    }

    public ArchiveTransfer getArchiveTransfer(DAOTransactionContext context, UUID archiveTransferUuid, boolean forUpdate,
                                              boolean includeRelativePaths, boolean includeArchiveTransferLog) throws DAOException {
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

            if((archiveTransfer != null) && (includeArchiveTransferLog)) {
                archiveTransfer.setTransferLogEntries(getTransferLogEntries(context, archiveTransfer.getId()));
            }

            return archiveTransfer;
        } catch (SQLException ex) {
            throw new DAOException(LibUtils.getMsg("FILES_TXFR_DAO_ERR_GENERAL", "getArchiveTransfer", ex.getMessage()), ex);
        }
    }

    public ArchiveTransfer updateArchiveTransfer(DAOTransactionContext context, ArchiveTransfer archiveTransfer,
                                                 boolean includeRelativePaths, boolean includeTransferLog) throws DAOException {
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
                    archiveTransfer.getRetriesRemaining(),
                    (archiveTransfer.getNextRetry() == null) ? null : Timestamp.from(archiveTransfer.getNextRetry()),
                    (archiveTransfer.getStartTime() == null) ? null : Timestamp.from(archiveTransfer.getStartTime()),
                    (archiveTransfer.getEndTime() == null) ? null : Timestamp.from(archiveTransfer.getEndTime()),
                    archiveTransfer.getAssignedTo(),
                    archiveTransfer.getUuid()
                    );

            if(includeRelativePaths) {
                updatedArchiveTransfer.setRelativePaths(getRelativePaths(context, archiveTransfer.getId()));
            }

            if(includeTransferLog) {
                updatedArchiveTransfer.setTransferLogEntries(updateTransferLogEntries(context,
                        updatedArchiveTransfer.getId(), archiveTransfer.getTransferLogEntries()));
            }

            return updatedArchiveTransfer;
        } catch (SQLException ex) {
            throw new DAOException(LibUtils.getMsg("FILES_TXFR_DAO_ERR_GENERAL", "updateArchiveTransfer", ex.getMessage()), ex);
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
                    ArchiveTransferDAOStatements.GET_ACCEPTED_AND_RETRY_ARCHIVE_TRANSFERS_ASSIGNED_TO_WORKER,
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
                    ArchiveTransferDAOStatements.GET_ACCEPTED_AND_RETRY_ARCHIVE_TRANSFERS_FOR_TENANTS_AND_USERS,
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

    public void cleanupZombieArchiveTransferAssignments(DAOTransactionContext context) throws DAOException {
        try {
            // first unassign everything that's in a non-terminal state
            QueryRunner runner = new QueryRunner();
            final Array finalStates = context.getConnection().createArrayOf("text", ArchiveTransferStatus.getFinalStates().stream().map(value -> value.name()).toArray());
            int zombies = runner.update(context.getConnection(), ArchiveTransferDAOStatements.UNASSIGN_ZOMBIE_ASSIGNMENTS, finalStates);
            // now find unassigned tasks that are 'IN PROGRESS' and set them back to ACCEPTED
            runner.update(context.getConnection(), ArchiveTransferDAOStatements.SET_IN_PROGRESS_BUT_AVAILABLE_TASKS_BACK_TO_ACCEPTED);
            if(zombies > 0) {
                log.info(LibUtils.getMsg("FILES_TXFR_DAO_REASSIGNED_ZOMBIES", zombies, "archiveTransfers"));
            }
        } catch (SQLException ex) {
            throw new DAOException(LibUtils.getMsg("FILES_TXFR_DAO_ERR_GENERAL", "cleanupZombieParentAssignments", ex.getMessage()), ex);
        }
    }

    public Collection<ArchiveTransfer> getAssignedTasksInStatus(DAOTransactionContext context, UUID workerUuid,
                                                                  TransferTaskStatus status, boolean forUpdate) throws DAOException {
        try {
            RowProcessor rowProcessor = new ArchiveTransferRowProcessor();
            BeanListHandler<ArchiveTransfer> handler = new BeanListHandler<>(ArchiveTransfer.class, rowProcessor);

            QueryRunner runner = new QueryRunner();
            Collection<ArchiveTransfer> archiveTransfers;
            String stmt;
            if (forUpdate) {
                stmt = ArchiveTransferDAOStatements.GET_ASSIGNED_TASKS_IN_STATUS_FOR_UPDATE;
            } else {
                stmt = ArchiveTransferDAOStatements.GET_ASSIGNED_TASKS_IN_STATUS;
            }
            archiveTransfers = runner.query(context.getConnection(), stmt, handler, workerUuid, status.toString());


            return archiveTransfers;
        } catch (SQLException ex) {
            throw new DAOException(LibUtils.getMsg("FILES_TXFR_DAO_ERR_GENERAL", "getAssignedTasksInStatus", ex.getMessage()), ex);
        }

    }

}
