package edu.utexas.tacc.tapis.files.lib.dao.postits;

import static edu.utexas.tacc.tapis.files.gen.jooq.tables.FilesPostits.FILES_POSTITS;

import edu.utexas.tacc.tapis.files.gen.jooq.tables.records.FilesPostitsRecord;
import edu.utexas.tacc.tapis.files.lib.database.HikariConnectionPool;
import edu.utexas.tacc.tapis.files.lib.models.PostIt;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisDBConnectionException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shareddb.datasource.TapisDataSource;
import org.apache.commons.lang3.StringUtils;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class PostItsDAO {

    private static final Logger log = LoggerFactory.getLogger(PostItsDAO.class);

    /**
     * Create a new PostIt record in the database.  The following
     * fields are ignored and are auto-populated by this method:
     * <p/>
     * Id
     * Created
     * Updated
     * <p/>
     *
     * @param postIt
     *
     * @return the newly created PostIt.
     * @throws TapisException
     */
    public PostIt createPostIt(PostIt postIt) throws TapisException {
        Connection conn = null;
        FilesPostitsRecord postItRecord = null;
        try {
            conn = getConnection();
            DSLContext db = DSL.using(conn);

            // Insert the PostIt in the database.
            LocalDateTime now = localDateTimeOfInstant(Instant.now());
            Record insertRecord = db.insertInto(FILES_POSTITS).
                                    set(FILES_POSTITS.ID, UUID.randomUUID().toString()).
                                    set(FILES_POSTITS.SYSTEMID, postIt.getSystemId()).
                                    set(FILES_POSTITS.PATH, postIt.getPath()).
                                    set(FILES_POSTITS.ALLOWEDUSES, postIt.getAllowedUses()).
                                    set(FILES_POSTITS.TIMESUSED, postIt.getTimesUsed()).
                                    set(FILES_POSTITS.JWTUSER, postIt.getJwtUser()).
                                    set(FILES_POSTITS.JWTTENANTID, postIt.getJwtTenantId()).
                                    set(FILES_POSTITS.OWNER, postIt.getOwner()).
                                    set(FILES_POSTITS.TENANTID, postIt.getTenantId()).
                                    set(FILES_POSTITS.EXPIRATION, localDateTimeOfInstant(postIt.getExpiration())).
                                    set(FILES_POSTITS.CREATED, now).
                                    set(FILES_POSTITS.UPDATED, now).
                                    returningResult(FILES_POSTITS.asterisk()).
                                    fetchOne();
            if(insertRecord == null) {
                // Record must not have been inserted for some reason.  Normally this
                // would result in an exception, but just for completeness, handle this
                // case.
                String msg = LibUtils.getMsg("POSTITS_DAO_ERROR_ID", postIt.getId(), "No record returned on insert");
                log.error(msg);
                throw new TapisException(msg);
            }

            postItRecord = FILES_POSTITS.from(insertRecord);
            logRecord("Inserted Record:", postItRecord);
            LibUtils.closeAndCommitDB(conn, null, null);
        } catch (Exception e) {
            // Rollback transaction and throw an exception
            LibUtils.rollbackDB(conn, e,"POSTITS_DAO_ERROR_ID", postIt.getId(), e.getMessage());
        } finally {
            // Always return the connection back to the connection pool.
            LibUtils.finalCloseDB(conn);
        }

        return createPostItFromRecord(postItRecord);
    }

    /**
     * Get a single PostIt by Id.
     *
     * @param postItId id of the PostIt to retrieve.
     * @return the PostIt record specified by the Id, or null if a PostIt
     * with the specified Id does not exist.
     * @throws TapisException
     */
    public PostIt getPostIt(String postItId) throws TapisException {
        Connection conn = null;
        FilesPostitsRecord postItRecord = null;
        try {
            conn = getConnection();
            DSLContext db = DSL.using(conn);
            Record selectRecord = db.select().from(FILES_POSTITS).
                    where(FILES_POSTITS.ID.eq(postItId)).
                    fetchOne();

            if(selectRecord != null) {
                postItRecord = FILES_POSTITS.from(selectRecord);
                logRecord("Retrieved Record:", postItRecord);
            }

            LibUtils.closeAndCommitDB(conn, null, null);
        } catch (Exception e) {
            // Rollback transaction and throw an exception
            LibUtils.rollbackDB(conn, e,"POSTITS_DAO_ERROR_ID",
                    postItId, e.getMessage());
        } finally {
            // Always return the connection back to the connection pool.
            LibUtils.finalCloseDB(conn);
        }

        return createPostItFromRecord(postItRecord);
    }

    /**
     * Returns a list of PostIts based on the condition passed in.
     *
     * @param tenantId The Id of the tenant to list PostIts for
     * @param ownerId The Id of the owner to list PostIts for - if no owner is specified,
     *               all PostIts for the tenant will be returned.
     * @return returns a non-null (but possibly empty) list of PostIts matching
     * the criteria in the condition.
     * @throws TapisException
     */
    public List<PostIt> listPostIts(String tenantId, String ownerId) throws TapisException {
        List<PostIt> postIts = new ArrayList<PostIt>();
        Connection conn = null;

        try {
            conn = getConnection();
            DSLContext db = DSL.using(conn);
            List<Record> selectRecords;

            if(StringUtils.isBlank(ownerId)) {
                selectRecords = db.select().from(FILES_POSTITS).
                        where(FILES_POSTITS.TENANTID.equal(tenantId)).
                        fetch();
            } else {
                selectRecords = db.select().from(FILES_POSTITS).
                        where(FILES_POSTITS.TENANTID.equal(tenantId)).
                        and(FILES_POSTITS.OWNER.equal(ownerId)).
                        fetch();
            }

            postIts = selectRecords.stream().map( record -> {
                FilesPostitsRecord postItRecord = FILES_POSTITS.from(record);
                return createPostItFromRecord(postItRecord);
            }).collect(Collectors.toList());

            LibUtils.closeAndCommitDB(conn, null, null);
        } catch (Exception e) {
            // Rollback transaction and throw an exception
            LibUtils.rollbackDB(conn, e,"POSTITS_DAO_LIST_ERROR", e.getMessage());
        } finally {
            // Always return the connection back to the connection pool.
            LibUtils.finalCloseDB(conn);
        }

        return postIts;
    }

    /**
     * Update's a PostIt.  The only fields that can be updated for a PostIt are:
     *
     * allowedUses
     * expiration
     *
     * All other fields will be ignored
     *
     * @param updatePostIt A PostIt containing the updated fields.  The ID of the
     *               PostIt must be set.
     *
     * @return the newly updated PostIt
     */
    public PostIt updatePostIt(PostIt updatePostIt) throws TapisException {
        Connection conn = null;
        FilesPostitsRecord postItRecord = null;

        try {
            conn = getConnection();
            DSLContext db = DSL.using(conn);
            PostIt postIt = prepareUpdate(db, updatePostIt);
            Record updatedRecord = db.update(FILES_POSTITS).
                    set(FILES_POSTITS.ALLOWEDUSES, postIt.getAllowedUses()).
                    set(FILES_POSTITS.EXPIRATION, localDateTimeOfInstant(postIt.getExpiration())).
                    set(FILES_POSTITS.UPDATED, localDateTimeOfInstant(Instant.now())).
                    where(FILES_POSTITS.ID.equal(postIt.getId())).
                    returningResult().
                    fetchOne();

            if (updatedRecord != null) {
                postItRecord = FILES_POSTITS.from(updatedRecord);
                logRecord("Updated Record:", postItRecord);
            }

            LibUtils.closeAndCommitDB(conn, null, null);
        } catch (Exception e) {
            // Rollback transaction and throw an exception
            LibUtils.rollbackDB(conn, e,"POSTITS_DAO_ERROR_ID",
                    updatePostIt.getId(), e.getMessage());
        } finally {
            // Always return the connection back to the connection pool.
            LibUtils.finalCloseDB(conn);
        }

        return createPostItFromRecord(postItRecord);
    }

    /**
     * Increments the use count for a PostIt.  The PostIt will be read (locked), and
     * checked for expiration (both by time and by usage count).  If the postIt is
     * redeemable now, the count will be incremented, and the postIt will be returned.
     * If the PostIt is expired or is not found, the method will return null.
     *
     * @param postItId
     * @return true if the PostItUseCount was updated, or false if it was not redeemable.
     * @throws TapisException
     */
    public boolean incrementUseIfRedeemable(String postItId) throws TapisException {
        Connection conn = null;
        FilesPostitsRecord updatedPostItRecord = null;
        try {
            conn = getConnection();
            DSLContext db = DSL.using(conn);

            PostIt currentPostIt = getPostItForUpdate(db, postItId);
            if(!currentPostIt.isRedeemable()) {
                return false;
            }
            if (currentPostIt.isRedeemable()) {
                Record updateRecord = db.update(FILES_POSTITS).
                        set(FILES_POSTITS.TIMESUSED, FILES_POSTITS.TIMESUSED.plus(1)).
                        set(FILES_POSTITS.UPDATED, localDateTimeOfInstant(Instant.now())).
                        where(FILES_POSTITS.ID.equal(postItId)).
                        returningResult(FILES_POSTITS.asterisk()).
                        fetchOne();

                if (updateRecord == null) {
                    // We should be able to update this record, so if something
                    // happens and we get nothing back, something bad has happened.
                    String msg = LibUtils.getMsg("POSTIT_NOT_FOUND", postItId);
                    log.error(msg);
                    throw new TapisException(msg);
                }

                updatedPostItRecord = FILES_POSTITS.from(updateRecord);
                logRecord("Updated Record:", updatedPostItRecord);
            } else {
                String msg = LibUtils.getMsg("POSTIT_NOT_REDEEMABLE", postItId);
                log.warn(msg);
                throw new TapisException(msg);
            }
            LibUtils.closeAndCommitDB(conn, null, null);
        } catch (Exception e) {
            // Rollback transaction and throw an exception
            LibUtils.rollbackDB(conn, e,"POSTITS_DAO_ERROR_ID", postItId, e.getMessage());
        } finally {
            // Always return the connection back to the connection pool.
            LibUtils.finalCloseDB(conn);
        }

        return true;
    }

    // Only support updating allowedUses, expiration, and usage
    private PostIt getPostItForUpdate(DSLContext db, String postItId)
            throws TapisException {
        Record record = db.select().from(FILES_POSTITS).
                where(FILES_POSTITS.ID.equal(postItId)).
                forUpdate().
                fetchOne();

        if(record == null) {
            // the postit that we are attempting to increment the usage count for
            // does not exist.
            String msg = LibUtils.getMsg("POSTIT_NOT_FOUND", postItId);
            log.error(msg);
            throw new TapisException(msg);
        }

        FilesPostitsRecord postitsRecord = FILES_POSTITS.from(record);
        PostIt postIt = createPostItFromRecord(postitsRecord);

        return postIt;
    }

    // Only support updating allowedUses and expiration
    // NOTE - this reads FOR UPDATE (meaning it's locked, and
    // the current transaction must be committed or rolled back
    // by the caller.
    private PostIt prepareUpdate(DSLContext db, PostIt updatePostIt)
            throws TapisException {
        PostIt postIt = getPostItForUpdate(db, updatePostIt.getId());
        if(updatePostIt.getAllowedUses() != null) {
            postIt.setAllowedUses(updatePostIt.getAllowedUses());
        }
        if(updatePostIt.getExpiration() != null) {
            postIt.setExpiration(updatePostIt.getExpiration());
        }
        return postIt;
    }

    private static synchronized Connection getConnection()
            throws TapisException
    {
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


    // Creates a PostIt that can be returned from the DAO given a record read from the database
    private PostIt createPostItFromRecord(FilesPostitsRecord record) {
        if(record == null) {
            return null;
        }

        PostIt postIt = new PostIt();
        postIt.setId(record.getId());
        postIt.setSystemId(record.getSystemid());
        postIt.setPath(record.getPath());
        postIt.setExpiration(instantOfLocalDateTime(record.getExpiration()));
        postIt.setAllowedUses(record.getAlloweduses());
        postIt.setOwner(record.getOwner());
        postIt.setTenantId(record.getTenantid());
        postIt.setJwtUser(record.getJwtuser());
        postIt.setJwtTenantId(record.getJwttenantid());
        postIt.setTimesUsed(record.getTimesused());
        postIt.setCreated(instantOfLocalDateTime(record.getCreated()));
        postIt.setUpdated(instantOfLocalDateTime(record.getUpdated()));
        return postIt;
    }

    // Trace logging for a db record
    private void logRecord(String msg, FilesPostitsRecord record) {
        log.trace(msg);
        log.trace(record == null ? null : record.toString());
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
