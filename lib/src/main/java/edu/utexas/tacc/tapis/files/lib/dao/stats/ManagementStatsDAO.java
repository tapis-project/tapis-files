package edu.utexas.tacc.tapis.files.lib.dao.stats;

import edu.utexas.tacc.tapis.files.gen.jooq.tables.TransferTasksParent;
import edu.utexas.tacc.tapis.files.gen.jooq.tables.records.SshSessionLogRecord;
import edu.utexas.tacc.tapis.files.gen.jooq.tables.records.TransferTasksChildRecord;
import edu.utexas.tacc.tapis.files.gen.jooq.tables.records.TransferTasksParentRecord;
import edu.utexas.tacc.tapis.files.gen.jooq.tables.records.TransferTasksRecord;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.DAOTransactionContext;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import edu.utexas.tacc.tapis.files.lib.models.managementStats.ChildTaskInfo;
import edu.utexas.tacc.tapis.files.lib.models.managementStats.ConnectionDetails;
import edu.utexas.tacc.tapis.files.lib.models.managementStats.ParentTaskInfo;
import edu.utexas.tacc.tapis.files.lib.models.managementStats.StatusSummary;
import edu.utexas.tacc.tapis.files.lib.models.managementStats.TopTaskInfo;
import edu.utexas.tacc.tapis.shared.ssh.stats.SessionPoolDetails;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import org.jooq.CommonTableExpression;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Select;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static edu.utexas.tacc.tapis.files.gen.jooq.tables.TransferTasksParent.TRANSFER_TASKS_PARENT;
import static edu.utexas.tacc.tapis.files.gen.jooq.tables.TransferTasksChild.TRANSFER_TASKS_CHILD;
import static edu.utexas.tacc.tapis.files.gen.jooq.tables.TransferTasks.TRANSFER_TASKS;
import static edu.utexas.tacc.tapis.files.gen.jooq.tables.SshSessionLog.SSH_SESSION_LOG;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.select;
import static org.jooq.impl.DSL.rowNumber;
import static org.jooq.impl.DSL.partitionBy;

public class ManagementStatsDAO {
    private static final Logger log = LoggerFactory.getLogger(ManagementStatsDAO.class);
    public enum AssignmentStatus {
        ASSIGNED,
        UNASSIGNED,
        ALL
    }

    public StatusSummary getSummary(DAOTransactionContext context) throws DAOException {
        DSLContext db = DSL.using(context.getConnection());

        StatusSummary statusSummary = new StatusSummary();
        Select<Record> inProgressTopTaskBaseQuery = getTopTasksInStatusBaseQuery(db, List.of( TransferTaskStatus.IN_PROGRESS), Optional.empty());
        statusSummary.setInProgressTopTasks(db.fetchCount(inProgressTopTaskBaseQuery));
        Select<Record> inProgressParentTaskBaseQuery = getParentTasksInStatusBaseQuery(db, Optional.of(List.of( TransferTaskStatus.IN_PROGRESS )), AssignmentStatus.ALL);
        statusSummary.setInProgressParentTasks(db.fetchCount(inProgressParentTaskBaseQuery));
        Select<Record> inProgressChildTaskBaseQuery = getChildTasksInStatusBaseQuery(db, Optional.of(List.of( TransferTaskStatus.IN_PROGRESS )), AssignmentStatus.ALL);
        statusSummary.setInProgressChildTasks(db.fetchCount(inProgressChildTaskBaseQuery));

        Select<Record> assignedParentTaskBaseQuery = getParentTasksInStatusBaseQuery(db,
                Optional.empty(),
                AssignmentStatus.ASSIGNED);
        statusSummary.setAssignedParentTasks(db.fetchCount(assignedParentTaskBaseQuery));
        Select<Record> assignedChildTaskBaseQuery = getChildTasksInStatusBaseQuery(db,
                Optional.empty(),
                AssignmentStatus.ASSIGNED);
        statusSummary.setAssignedChildTasks(db.fetchCount(assignedChildTaskBaseQuery));

        Select<Record> unassignedParentTaskBaseQuery = getParentTasksInStatusBaseQuery(db,
                Optional.of(List.of(TransferTaskStatus.ACCEPTED, TransferTaskStatus.AWAITING_RETRY, TransferTaskStatus.STAGING)),
                AssignmentStatus.UNASSIGNED);
        statusSummary.setUnassignedParentTasks(db.fetchCount(unassignedParentTaskBaseQuery));
        Select<Record> unassignedChildTaskBaseQuery = getChildTasksInStatusBaseQuery(db,
                Optional.of(List.of(TransferTaskStatus.ACCEPTED, TransferTaskStatus.IN_PROGRESS, TransferTaskStatus.AWAITING_RETRY )),
                AssignmentStatus.UNASSIGNED);
        statusSummary.setUnassignedChildTasks(db.fetchCount(unassignedChildTaskBaseQuery));

        Select<Record> awaitingRetryParentTaskBaseQuery = getParentTasksInStatusBaseQuery(db,
                Optional.of( List.of(TransferTaskStatus.AWAITING_RETRY) ),
                AssignmentStatus.ALL);
        statusSummary.setAwaitingRetryParentTasks(db.fetchCount(awaitingRetryParentTaskBaseQuery));
        Select<Record> awaitingRetryChildTaskBaseQuery = getChildTasksInStatusBaseQuery(db,
                Optional.of( List.of(TransferTaskStatus.AWAITING_RETRY) ),
                AssignmentStatus.ALL);
        statusSummary.setAwaitingRetryChildTasks(db.fetchCount(awaitingRetryChildTaskBaseQuery));

        return statusSummary;
    }

    // top queries
    private Select<Record> getTopTasksInStatusBaseQuery(DSLContext db, List<TransferTaskStatus> statuses, Optional<Integer> limit) {
        var query = db.select().from(TRANSFER_TASKS).
                where(TRANSFER_TASKS.STATUS.in(statuses));
        if(limit.isPresent()) {
            return query.limit(limit.get());
        } else {
            return query;
        }
    }

    public List<TopTaskInfo> getTopTasksInStatus(DAOTransactionContext context, List<TransferTaskStatus> statuses, Optional<Integer> limit) throws DAOException {
        List<TopTaskInfo> topTaskInfos = null;
        DSLContext db = DSL.using(context.getConnection());
        Select<Record> baseQuery = getTopTasksInStatusBaseQuery(db, statuses, limit);
        List<TransferTasksRecord> result = baseQuery.
                fetchInto(TransferTasksRecord.class);
        if (result != null) {
            topTaskInfos = result.stream().map(top -> {
                return topInfoFromTopTask(top);
            }).collect(Collectors.toList());
        }

        return topTaskInfos;
    }

    public TopTaskInfo getTopTaskByTaskId(DAOTransactionContext context, int taskId) throws DAOException {
        DSLContext db = DSL.using(context.getConnection());
        TransferTasksRecord ttr = db.select().from(TRANSFER_TASKS).
                where(TRANSFER_TASKS.ID.eq(taskId)).fetchOneInto(TransferTasksRecord.class);
        return topInfoFromTopTask(ttr);
    }

    public TopTaskInfo getTopTaskByUuid(DAOTransactionContext context, UUID uuid) throws DAOException {
        DSLContext db = DSL.using(context.getConnection());
        TransferTasksRecord ttr = db.select().from(TRANSFER_TASKS).
                where(TRANSFER_TASKS.UUID.eq(uuid)).fetchOneInto(TransferTasksRecord.class);
        return topInfoFromTopTask(ttr);
    }

    // parent queries
    private Select<Record> getParentTasksInStatusBaseQuery(DSLContext db, Optional<List<TransferTaskStatus>> statuses, AssignmentStatus assignmentStatus) throws DAOException {
        return db.select().from(TRANSFER_TASKS_PARENT).where(
                statuses.isPresent() ? TRANSFER_TASKS_PARENT.STATUS.in(statuses.get()) : DSL.noCondition(),
                assignmentStatus.equals(AssignmentStatus.ASSIGNED) ? TRANSFER_TASKS_PARENT.ASSIGNED_TO.isNotNull() : DSL.noCondition(),
                assignmentStatus.equals(AssignmentStatus.UNASSIGNED) ? TRANSFER_TASKS_PARENT.ASSIGNED_TO.isNull() : DSL.noCondition()
        );
    }

    public List<ParentTaskInfo> getParentTasksInStatus(DAOTransactionContext context, List<TransferTaskStatus> statuses) throws DAOException {
        List<ParentTaskInfo> parentTaskInfos = null;
        DSLContext db = DSL.using(context.getConnection());
        Select<Record> baseQuery = getParentTasksInStatusBaseQuery(db, Optional.of(statuses), AssignmentStatus.ALL);
        List<TransferTasksParentRecord> result = baseQuery.
                fetchInto(TransferTasksParentRecord.class);

        if (result != null) {
            parentTaskInfos = result.stream().map(parent -> {
                return parentInfoFromParentTask(parent);
            }).collect(Collectors.toList());
        }

        return parentTaskInfos;
    }

    public List<ParentTaskInfo> getParentTasks(DAOTransactionContext context, List<Integer> parentTasksToRetrieve) throws DAOException {
        List<ParentTaskInfo> parentTaskInfos = null;
        DSLContext db = DSL.using(context.getConnection());
        List<TransferTasksParentRecord> result = db.select().from(TRANSFER_TASKS_PARENT).
                where(TRANSFER_TASKS_PARENT.ID.in(parentTasksToRetrieve)).
                fetchInto(TransferTasksParentRecord.class);

        if (result != null) {
            parentTaskInfos = result.stream().map(parent -> {
                return parentInfoFromParentTask(parent);
            }).collect(Collectors.toList());
        }

        return parentTaskInfos;
    }

    public List<ParentTaskInfo> getParentTasksByTopTaskId(DAOTransactionContext context, int topTaskId, int limit) throws DAOException {
        List<ParentTaskInfo> parentTaskInfos = null;
        DSLContext db = DSL.using(context.getConnection());
        List<TransferTasksParentRecord> result = db.select().
                from(TRANSFER_TASKS_CHILD).where(TRANSFER_TASKS_CHILD.TASK_ID.eq(topTaskId)).
                limit(limit).
                fetchInto(TransferTasksParentRecord.class);

        if (result != null) {
            parentTaskInfos = result.stream().map(parent -> {
                return parentInfoFromParentTask(parent);
            }).collect(Collectors.toList());
        }

        return parentTaskInfos;
    }

    // child queries
    private Select<Record> getChildTasksInStatusBaseQuery(DSLContext db, Optional<List<TransferTaskStatus>> statuses, AssignmentStatus assignmentStatus) throws DAOException {
        return db.select().from(TRANSFER_TASKS_CHILD).where(
                statuses.isPresent() ? TRANSFER_TASKS_CHILD.STATUS.in(statuses.get()) : DSL.noCondition(),
                assignmentStatus.equals(AssignmentStatus.ASSIGNED) ? TRANSFER_TASKS_CHILD.ASSIGNED_TO.isNotNull() : DSL.noCondition(),
                assignmentStatus.equals(AssignmentStatus.UNASSIGNED) ? TRANSFER_TASKS_CHILD.ASSIGNED_TO.isNull() : DSL.noCondition()
        );
    }

    public List<ChildTaskInfo> getChildTasksInStatus(DAOTransactionContext context, List<TransferTaskStatus> statuses) throws DAOException {
        List<ChildTaskInfo> childTaskInfos = null;
        DSLContext db = DSL.using(context.getConnection());
        Select<Record> baseQuery = getChildTasksInStatusBaseQuery(db, Optional.of(statuses), AssignmentStatus.ALL);
        List<TransferTasksChildRecord> result = baseQuery.
                fetchInto(TransferTasksChildRecord.class);
        if (result != null) {
            childTaskInfos = result.stream().map(child -> {
                return childInfoFromChildTask(child);
            }).collect(Collectors.toList());
        }

        return childTaskInfos;
    }

    public List<ChildTaskInfo> getChildTasks(DAOTransactionContext context, List<Integer> childTasksToRetrieve) throws DAOException {
        List<ChildTaskInfo> childTaskInfos = null;
        DSLContext db = DSL.using(context.getConnection());
        List<TransferTasksChildRecord> result = db.select().from(TRANSFER_TASKS_CHILD).
                where(TRANSFER_TASKS_CHILD.ID.in(childTasksToRetrieve)).
                fetchInto(TransferTasksChildRecord.class);

        if (result != null) {
            childTaskInfos = result.stream().map(child -> {
                return childInfoFromChildTask(child);
            }).collect(Collectors.toList());
        }

        return childTaskInfos;
    }

    public List<ChildTaskInfo> getChildTasksByTopTaskId(DAOTransactionContext context, int topTaskId, int limit) throws DAOException {
        List<ChildTaskInfo> childTaskInfos = null;
        DSLContext db = DSL.using(context.getConnection());
        List<TransferTasksChildRecord> result = db.select().
                from(TRANSFER_TASKS_CHILD).where(TRANSFER_TASKS_CHILD.TASK_ID.eq(topTaskId)).
                limit(limit).
                fetchInto(TransferTasksChildRecord.class);

        if (result != null) {
            childTaskInfos = result.stream().map(child -> {
                return childInfoFromChildTask(child);
            }).collect(Collectors.toList());
        }

        return childTaskInfos;
    }

    // Ssh status queries
    public int saveSshStats(DAOTransactionContext context, ConnectionDetails details) {
        DSLContext db = DSL.using(context.getConnection());
        Record insertedRecord = db.insertInto(SSH_SESSION_LOG)
                .set(SSH_SESSION_LOG.SESSION_STATS, JSONB.jsonbOrNull(TapisGsonUtils.getGson().toJson(details.getSessionPoolDetails())))
                .set(SSH_SESSION_LOG.CONTEXT_NAME, details.getContextName())
                .returningResult(SSH_SESSION_LOG.asterisk())
                .fetchOne();
        SshSessionLogRecord sshSessionLogRecord = SSH_SESSION_LOG.from(insertedRecord);
        return 0;
    }

    public List<ConnectionDetails> getSshStats(DAOTransactionContext context) {
        DSLContext db = DSL.using(context.getConnection());

        Field<Integer> rowNumberField = rowNumber()
                .over(
                        partitionBy(SSH_SESSION_LOG.CONTEXT_NAME)
                                .orderBy(SSH_SESSION_LOG.CREATED.desc())
                )
                .as("row_number");

        CommonTableExpression<Record> sessions = name("sessions").as(
                select(SSH_SESSION_LOG.asterisk(), rowNumberField)
                        .from(SSH_SESSION_LOG)
        );

        return db.with(sessions)
                .selectFrom(sessions)
                .where(sessions.field("row_number", Integer.class).eq(1))
                .fetch().stream().map(record -> {
                    SshSessionLogRecord logRecord = SSH_SESSION_LOG.from(record);
                    ConnectionDetails connectionDetails = new ConnectionDetails();
                    connectionDetails.setContextName(logRecord.getContextName());
                    String jsonString = logRecord.getSessionStats().toString();
                    SessionPoolDetails details = TapisGsonUtils.getGson().fromJson(jsonString,  SessionPoolDetails.class);
                    connectionDetails.setSessionPoolDetails(details);
                    connectionDetails.setUpdated(instantOfLocalDateTime(logRecord.getUpdated()));
                    connectionDetails.setCreated(instantOfLocalDateTime(logRecord.getCreated()));
                    return connectionDetails;
                }).toList();
    }

    public int deleteOlderThan(DAOTransactionContext context, String contextName, Instant time) {
        DSLContext db = DSL.using(context.getConnection());
        return db.delete(SSH_SESSION_LOG)
                .where(SSH_SESSION_LOG.UPDATED.lessThan(LocalDateTime.ofInstant(time, ZoneOffset.UTC)))
                .execute();
    }

    // conversion methods
    private ParentTaskInfo parentInfoFromParentTask(TransferTasksParentRecord task) {
        Instant nextRetry = (task.getNextRetry() == null) ? null : task.getNextRetry().toInstant();
        ParentTaskInfo parentTaskInfo = new ParentTaskInfo(task.getId(), task.getUuid(), task.getStatus(),
                task.getTaskId(), task.getTenantId(), task.getUsername(), task.getRetriesRemaining(),
                nextRetry, task.getCreated().toInstant());
        parentTaskInfo.setErrorMessage(task.getErrorMessage());
        parentTaskInfo.setAssignedTo(task.getAssignedTo());
        return parentTaskInfo;
    }

    private ChildTaskInfo childInfoFromChildTask(TransferTasksChildRecord task) {
        Instant nextRetry = (task.getNextRetry() == null) ? null : task.getNextRetry().toInstant();
        ChildTaskInfo childTaskInfo = new ChildTaskInfo(task.getId(), task.getUuid(), task.getStatus(),
                task.getTaskId(), task.getParentTaskId(), task.getTenantId(), task.getUsername(),
                task.getRetriesRemaining(), task.getNextRetry() == null ? null : task.getNextRetry().toInstant(), task.getCreated().toInstant());
        childTaskInfo.setErrorMessage(task.getErrorMessage());
        childTaskInfo.setAssignedTo(task.getAssignedTo());
        return childTaskInfo;
    }

    private TopTaskInfo topInfoFromTopTask(TransferTasksRecord task) {
        TopTaskInfo topTaskInfo = new TopTaskInfo(task.getId(), task.getUuid(), task.getStatus(),
                task.getTenantId(), task.getUsername(), task.getCreated().toInstant());
        topTaskInfo.setErrorMessage(task.getErrorMessage());
        return topTaskInfo;
    }

    private Instant instantOfLocalDateTime(LocalDateTime ldt) {
        if(ldt == null) {
            return null;
        }

        return ldt.toInstant(ZoneOffset.UTC);
    }

    private LocalDateTime localDateTimeOfInstant(Instant instant) {
        if(instant == null) {
            return null;
        }

        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

}

