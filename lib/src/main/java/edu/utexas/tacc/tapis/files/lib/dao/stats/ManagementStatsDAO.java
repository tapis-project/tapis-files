package edu.utexas.tacc.tapis.files.lib.dao.stats;

import edu.utexas.tacc.tapis.files.client.gen.model.TransferTaskParent;
import edu.utexas.tacc.tapis.files.gen.jooq.tables.records.TransferTasksParentRecord;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.DAOTransactionContext;
import edu.utexas.tacc.tapis.files.lib.database.HikariConnectionPool;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import edu.utexas.tacc.tapis.files.lib.models.managementStats.ParentTaskInfo;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisDBConnectionException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shareddb.datasource.TapisDataSource;
import org.jooq.DSLContext;
import org.jooq.Null;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import static edu.utexas.tacc.tapis.files.gen.jooq.tables.TransferTasksParent.TRANSFER_TASKS_PARENT;

public class ManagementStatsDAO {
    private static final Logger log = LoggerFactory.getLogger(ManagementStatsDAO.class);

    public List<ParentTaskInfo> getParentTasks(DAOTransactionContext context, List<Integer> parentTaskToRetrieve) throws DAOException {
        List<ParentTaskInfo> parentTaskInfos = null;
            DSLContext db = DSL.using(context.getConnection());
            List<TransferTasksParentRecord> result = db.select().from(TRANSFER_TASKS_PARENT).
                    where(TRANSFER_TASKS_PARENT.ID.in(parentTaskToRetrieve)).
                    fetchInto(TransferTasksParentRecord.class);

            if (result != null) {
                parentTaskInfos = result.stream().map(parent -> {
                    return parentInfoFromParentTask(parent);
                }).collect(Collectors.toList());
            }

        return parentTaskInfos;
    }
    public List<ParentTaskInfo> getParentTasksInStatus(DAOTransactionContext context, List<TransferTaskStatus> statuses) throws DAOException {
        List<ParentTaskInfo> parentTaskInfos = null;
        DSLContext db = DSL.using(context.getConnection());
        List<TransferTasksParentRecord> result = db.select().from(TRANSFER_TASKS_PARENT).
                where(TRANSFER_TASKS_PARENT.STATUS.in(statuses)).
                fetchInto(TransferTasksParentRecord.class);

        if (result != null) {
            parentTaskInfos = result.stream().map(parent -> {
                return parentInfoFromParentTask(parent);
            }).collect(Collectors.toList());
        }

        return parentTaskInfos;
    }


    private static synchronized Connection getConnection() throws TapisException {
        // Use the existing datasource.
        DataSource ds = getDataSource();

        // Get the connection.
        Connection conn = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);
        } catch (Exception e) {
            String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION");
            log.error(msg, e);
            throw new TapisDBConnectionException(msg, e);
        }

        return conn;
    }
    private static DataSource getDataSource() throws TapisException
    {
        // Use the existing datasource.
        DataSource ds = TapisDataSource.getDataSource();
        if (ds == null) {
            // Get a database connection.
            ds = HikariConnectionPool.getDataSource();
        }

        return ds;
    }
    private ParentTaskInfo parentInfoFromParentTask(TransferTasksParentRecord task) {
        Instant nextRetry = (task.getNextRetry() == null) ? null : task.getNextRetry().toInstant();
        ParentTaskInfo parentTaskInfo = new ParentTaskInfo(task.getId(), task.getStatus(), task.getTaskId(), task.getTenantId(),
                task.getUsername(), task.getRetriesRemaining(), nextRetry, task.getCreated().toInstant());
        parentTaskInfo.setErrorMessage(task.getErrorMessage());
        parentTaskInfo.setAssignedTo(task.getAssignedTo());
        return parentTaskInfo;
    }

}

