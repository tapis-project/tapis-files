package edu.utexas.tacc.tapis.files.lib.dao.transfers;

import edu.utexas.tacc.tapis.files.lib.database.HikariConnectionPool;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.models.PrioritizedObject;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.RowProcessor;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class TransferTaskChildDAO {

    Logger log = LoggerFactory.getLogger(TransferTaskChildDAO.class);

    public List<PrioritizedObject<TransferTaskChild>> getAcceptedChildTasksForTenantsAndUsers(DAOTransactionContext context, int maxTasksPerTenantAndUser) throws DAOException {
        RowProcessor rowProcessor = new PrioritizedObjectRowProcessor(new TransferTaskChildRowProcessor(), TransferTaskChild.class);

        try {
            ResultSetHandler<List<PrioritizedObject>> handler =
                    new BeanListHandler<PrioritizedObject>(PrioritizedObject.class, rowProcessor);

            QueryRunner runner = new QueryRunner();
            List<PrioritizedObject> children = runner.query(
                    context.getConnection(),
                    TransferTaskChildDAOStatements.GET_ACCEPTED_CHILD_TASKS_FOR_TENANTS_AND_USERS,
                    handler,
                    maxTasksPerTenantAndUser);

            return (List<PrioritizedObject<TransferTaskChild>>)(Object)children;
        } catch (SQLException ex) {
            throw new DAOException(LibUtils.getMsg("FILES_TXFR_DAO_ERR_GENERAL", "getAcceptedChildTasksForTenantsAndUsers", ex.getMessage()), ex);
        }
    }

    public List<PrioritizedObject<TransferTaskChild>> getAssignedWorkForWorker(DAOTransactionContext context, int maxTasksPerTenantAndUser, UUID workerUuid) throws DAOException {
        RowProcessor rowProcessor = new PrioritizedObjectRowProcessor(new TransferTaskChildRowProcessor(), TransferTaskChild.class);

        try {
            ResultSetHandler<List<PrioritizedObject>> handler =
                    new BeanListHandler<PrioritizedObject>(PrioritizedObject.class, rowProcessor);

            QueryRunner runner = new QueryRunner();
            List<PrioritizedObject> children;
            children = runner.query(
                    context.getConnection(),
                    TransferTaskChildDAOStatements.GET_ACCEPTED_CHILD_TASKS_ASSIGNED_TO_WORKER,
                    handler,
                    workerUuid,
                    maxTasksPerTenantAndUser);


            return (List<PrioritizedObject<TransferTaskChild>>) (Object) children;
        } catch (SQLException ex) {
            throw new DAOException(LibUtils.getMsg("FILES_TXFR_DAO_ERR_GENERAL", "getAssignedWorkForWorker", ex.getMessage()), ex);
        }
    }


    public Map<UUID, Integer> getAssignedWorkerCount(DAOTransactionContext context) throws DAOException {
        Map<UUID, Integer> assignedWorkerCount = new HashMap<>();
        try {
            String sql = TransferTaskChildDAOStatements.GET_ASSIGNED_CHILD_COUNT;
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
        if((workerId == null)  || (taskIds.isEmpty())) {
            // TODO: fix this warning message.  It could be no worker id or could be no taskIds
            log.warn(LibUtils.getMsg("FILES_TXFR_DAO_NO_WORKER_PROVIDED", "assignToWorkers"));
            return;
        }

        try {
            QueryRunner runner = new QueryRunner();
            final Array tasksToUpdate = context.getConnection().createArrayOf("int", taskIds.toArray());
            int numberAssigned = runner.update(context.getConnection(), TransferTaskChildDAOStatements.ASSIGN_TASKS_TO_WORKER, workerId, tasksToUpdate);
            log.info(LibUtils.getMsg("FILES_TXFR_DAO_ASSIGNED", numberAssigned, taskIds.size(), workerId));
        } catch (SQLException ex) {
            throw new DAOException(LibUtils.getMsg("FILES_TXFR_DAO_ERR_GENERAL", "assignToWorkers", ex.getMessage()), ex);
        }
    }

    public void cleanupZombieChildAssignments(DAOTransactionContext context, Set<TransferTaskStatus> terminalStates) throws DAOException {
        try {
            // first unassign everything that's in a non-terminal state
            QueryRunner runner = new QueryRunner();
            final Array finalStates = context.getConnection().createArrayOf("text", terminalStates.stream().map(value -> value.name()).toArray());
            int zombies = runner.update(context.getConnection(), TransferTaskChildDAOStatements.UNASSIGN_ZOMBIE_ASSIGNMENTS, finalStates);
            if(zombies > 0) {
                log.info("Reassigned child zombie tasks: " + zombies);
            }

            // Now change everything that is IN_PROGRESS, but not assigned back to 'ACCEPTED' ... this effectively restarts them
            runner = new QueryRunner();
            int restarted = runner.update(context.getConnection(), TransferTaskChildDAOStatements.RESTART_UNASSIGNED_BUT_IN_PROGRESS_TASKS);
            if(restarted > 0) {
                log.info("Restarted child tasks: " + restarted);
            }

        } catch (SQLException ex) {
            throw new DAOException(LibUtils.getMsg("FILES_TXFR_DAO_ERR_GENERAL", "cleanupZombieChildAssignments", ex.getMessage()), ex);
        }

    }
    public void bulkInsertChildTasks(DAOTransactionContext context, List<TransferTaskChild> children) throws DAOException {
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
                    child.getTotalBytes(),
                    child.isDir(),
                    child.getTag(),
                    child.getExternalTaskId()
            });
        });
        Object[][] t = new Object[params.size()][];
        params.toArray(t);
        try {
            String stmt = TransferTaskChildDAOStatements.INSERT_CHILD_TASK;
            QueryRunner runner = new QueryRunner();
            runner.batch(context.getConnection(), stmt, t);
        } catch (SQLException ex) {
            throw new DAOException("Bulk insert failed!", ex);
        }
    }

    public TransferTaskChild insertChildTask(DAOTransactionContext context, TransferTaskChild task) throws DAOException {
        RowProcessor rowProcessor = new TransferTaskChildRowProcessor();

        try {
            BeanHandler<TransferTaskChild> handler = new BeanHandler<>(TransferTaskChild.class, rowProcessor);
            String stmt = TransferTaskChildDAOStatements.INSERT_CHILD_TASK;
            QueryRunner runner = new QueryRunner();
            TransferTaskChild child = runner.query(context.getConnection(), stmt, handler,
                    task.getTenantId(),
                    task.getTaskId(),
                    task.getParentTaskId(),
                    task.getUsername(),
                    task.getSourceURI().toString(),
                    task.getDestinationURI().toString(),
                    task.getStatus().name(),
                    task.getBytesTransferred(),
                    task.getTotalBytes(),
                    task.isDir(),
                    task.getTag(),
                    task.getExternalTaskId()
            );

            return child;
        } catch (SQLException ex) {
            throw new DAOException(LibUtils.getMsg("FILES_TXFR_DAO_ERR1", task.getTenantId(), task.getUsername(),
                    "insertChildTask", task.getId(), task.getTag(), task.getUuid(), ex.getMessage()), ex);
        }
    }



}
