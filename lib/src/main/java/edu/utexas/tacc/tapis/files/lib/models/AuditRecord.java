package edu.utexas.tacc.tapis.files.lib.models;

import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.shared.utils.AuditUtils;
import edu.utexas.tacc.tapis.shared.utils.AuditUtils.AuditData;
import edu.utexas.tacc.tapis.shared.utils.AuditUtils.AUDIT_ACTION;
import edu.utexas.tacc.tapis.shared.utils.PathUtils;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.util.UUID;

/**
 * Class representing a record that Files service will write to the audit log.
 */
public class AuditRecord {

    // Audit data needed by AuditUtils
    private final AuditData auditData;

    /**
     * Initialize an audit record in the context of api or worker process.
     *
     * @param component - the service component, filesworker or filesapi
     * @param action - the action to log, e.g. mkdir, upload, copy, delete, etc
     * @param targetSystem - System containing the affected file
     * @param targetRelPath - Relative path of the affected file or directory
     * @param sourceSystem - Optional system containing the original file or directory. For txfr, copy, move actions
     * @param sourceRelPath - Relative path of original file or directory.
     * @param impersonationId - use provided Tapis username instead of oboUser
     * @param data - Optional additional information associated with the event, as json
     * @param trackingId - Optional audit tracking id. One is created if not provided here.
     */
    public AuditRecord(ResourceRequestUser rUser, String component, AUDIT_ACTION action, TapisSystem targetSystem,
                       String targetRelPath, TapisSystem sourceSystem, String sourceRelPath, String data,
                       String impersonationId, String trackingId) {
        String targetPathStr = PathUtils.getAbsolutePath(targetSystem.getRootDir(), targetRelPath).toString();
        String sourcePathStr = (sourceSystem == null) ? null : PathUtils.getAbsolutePath(sourceSystem.getRootDir(), sourceRelPath).toString();
        auditData = new AuditData();
        auditData.jwtTenant = rUser.getJwtTenantId();
        auditData.jwtUser = rUser.getJwtUserId();
        auditData.oboTenant = rUser.getOboTenantId();
        auditData.oboUser = StringUtils.isBlank(impersonationId) ? rUser.getOboTenantId() : impersonationId;
        auditData.component = component;
        auditData.action = action.name();
        auditData.targetSystemId = targetSystem.getId();
        auditData.targetSystemType = targetSystem.getSystemType().name();
        auditData.targetHost = targetSystem.getHost();
        auditData.targetPath = targetPathStr;

        auditData.sourceSystemId = (sourceSystem == null) ? AuditUtils.AUDIT_EMPTY : sourceSystem.getId();
        auditData.sourceSystemType = (sourceSystem == null) ? AuditUtils.AUDIT_EMPTY : sourceSystem.getSystemType().name();
        auditData.sourceHost = (sourceSystem == null) ? AuditUtils.AUDIT_EMPTY : sourceSystem.getHost();
        auditData.sourcePath = (sourceSystem == null || StringUtils.isBlank(sourcePathStr)) ? AuditUtils.AUDIT_EMPTY : sourcePathStr;
        auditData.trackingId = (!StringUtils.isBlank(trackingId)) ? trackingId : AuditUtils.TRACKING_PREFIX_FILES + UUID.randomUUID();
        auditData.parentTrackingId = TapisThreadLocal.tapisThreadContext.get().getTrackingId();
        auditData.data = StringUtils.isBlank(data) ? TapisConstants.EMPTY_JSON : data;
    }

    /*
     * Method to log and audit record
     */
    public static void log(Logger log, AuditRecord ar) {
        if (!RuntimeSettings.get().isAuditingEnabled()) return;
        AuditData ad = ar.getAuditData();
        log.info(AuditUtils.auditMsg(ad));
    }

    // Accessor
    public AuditData getAuditData() { return  auditData; }
}
