package edu.utexas.tacc.tapis.files.lib.dao.transfers;

import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.models.PrioritizedObject;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskParent;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.RowProcessor;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class TransferTaskParentDAO {
    Logger log = LoggerFactory.getLogger(TransferTaskParentDAO.class);

    public List<PrioritizedObject<TransferTaskParent>> getAcceptedParentTasksForTenantsAndUsers(DAOTransactionContext context, int maxTasksPerTenantAndUser) throws DAOException {
        RowProcessor rowProcessor = new PrioritizedObjectRowProcessor(new TransferTaskParentRowProcessor(), TransferTaskParent.class);

        try {
            ResultSetHandler<List<PrioritizedObject>> handler =
                    new BeanListHandler<PrioritizedObject>(PrioritizedObject.class, rowProcessor);

            QueryRunner runner = new QueryRunner();
            List<PrioritizedObject> parents = runner.query(
                    context.getConnection(),
                    TransferTaskParentDAOStatements.GET_ACCEPTED_PARENT_TASKS_FOR_TENANTS_AND_USERS,
                    handler,
                    maxTasksPerTenantAndUser);

            return (List<PrioritizedObject<TransferTaskParent>>)(Object)parents;
        } catch (SQLException ex) {
            throw new DAOException(LibUtils.getMsg("FILES_TXFR_DAO_ERR_GENERAL", "getAcceptedParentTasksForTenantsAndUsers", ex.getMessage()), ex);
        }
    }


    public Map<UUID, Integer> getAssignedWorkerCount(DAOTransactionContext context) throws DAOException {
        Map<UUID, Integer> assignedWorkerCount = new HashMap<>();
        try {
            String sql = TransferTaskParentDAOStatements.GET_ASSIGNED_PARENT_COUNT;
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

    public List<PrioritizedObject<TransferTaskParent>> getAssignedWorkForWorker(DAOTransactionContext context, int maxTasksPerTenantAndUser, UUID workerUuid) throws DAOException {
        RowProcessor rowProcessor = new PrioritizedObjectRowProcessor(new TransferTaskParentRowProcessor(), TransferTaskParent.class);

        try {
            ResultSetHandler<List<PrioritizedObject>> handler =
                    new BeanListHandler<PrioritizedObject>(PrioritizedObject.class, rowProcessor);

            QueryRunner runner = new QueryRunner();
            List<PrioritizedObject> parents = runner.query(
                    context.getConnection(),
                    TransferTaskParentDAOStatements.GET_ACCEPTED_PARENT_TASKS_ASSIGNED_TO_WORKER,
                    handler,
                    workerUuid,
                    maxTasksPerTenantAndUser);


            return (List<PrioritizedObject<TransferTaskParent>>) (Object) parents;
        } catch (SQLException ex) {
            throw new DAOException(LibUtils.getMsg("FILES_TXFR_DAO_ERR_GENERAL", "getAssignedWorkForWorker", ex.getMessage()), ex);
        }
    }

    public void assignToWorkers(DAOTransactionContext context, List<Integer> taskIds, UUID workerId) throws DAOException {
        if((workerId == null)  || (taskIds.isEmpty())) {
            log.warn(LibUtils.getMsg("FILES_TXFR_DAO_NO_WORKER_PROVIDED", "assignToWorkers"));
            return;
        }

        try {
            QueryRunner runner = new QueryRunner();
            final Array tasksToUpdate = context.getConnection().createArrayOf("long", taskIds.toArray());
            int numberAssigned = runner.update(context.getConnection(), TransferTaskParentDAOStatements.ASSIGN_TASKS_TO_WORKER, workerId, tasksToUpdate);
            log.info(LibUtils.getMsg("FILES_TXFR_DAO_ASSIGNED", numberAssigned, taskIds.size(), workerId));
        } catch (SQLException ex) {
            throw new DAOException(LibUtils.getMsg("FILES_TXFR_DAO_ERR_GENERAL", "assignToWorkers", ex.getMessage()), ex);
        }
    }

    public void cleanupZombieParentAssignments(DAOTransactionContext context, Set<TransferTaskStatus> terminalStates) throws DAOException {
        try {
            // first unassign everything that's in a non-terminal state
            QueryRunner runner = new QueryRunner();
            final Array finalStates = context.getConnection().createArrayOf("text", terminalStates.stream().map(value -> value.name()).toArray());
            int zombies = runner.update(context.getConnection(), TransferTaskParentDAOStatements.UNASSIGN_ZOMBIE_ASSIGNMENTS, finalStates);
            if(zombies > 0) {
                log.info("Reassigned parent zombie tasks: " + zombies);
            }


            // Now change everything that is IN_PROGRESS but unassigned, to failed (maybe we could do better one day, but this is OK for now).  If
            // we ever do decide to change this we have to figure out what to do about parent tasks in progress - the are creating child tasks, and
            // we want to avoid duplication.  Child tasks are pretty easy to restart, but parents would require more thought.
            Statement statement = context.getConnection().createStatement();
            ResultSet resultSet = statement.executeQuery(TransferTaskParentDAOStatements.FAIL_UNASSIGNED_BUT_IN_PROGRESS_TASKS);
            List<Long> topTasksAffected = new ArrayList<Long>();
            while(resultSet.next()) {
                topTasksAffected.add(resultSet.getLong("task_id"));
            }

            if(topTasksAffected.size() > 0) {
                log.info("Failed parent tasks: " + topTasksAffected.size());
                // if we failed a parent task, we must fail it's top task if this is a non-optional task
                runner = new QueryRunner();
                final Array topTaskIds = context.getConnection().createArrayOf("long", topTasksAffected.toArray());
                int failedTopTasks = runner.update(context.getConnection(), TransferTaskParentDAOStatements.FAIL_ASSOCIATED_TOP_TASKS, topTaskIds);
                if(failedTopTasks > 0) {
                    log.info("Failed top tasks: " + failedTopTasks);
                }
            }
        } catch (SQLException ex) {
            throw new DAOException(LibUtils.getMsg("FILES_TXFR_DAO_ERR_GENERAL", "cleanupZombieParentAssignments", ex.getMessage()), ex);
        }

    }

}
