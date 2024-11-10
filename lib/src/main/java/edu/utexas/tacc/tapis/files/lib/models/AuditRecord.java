package edu.utexas.tacc.tapis.files.lib.models;

import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.utils.AuditUtils;
import edu.utexas.tacc.tapis.shared.utils.AuditUtils.AuditData;
import edu.utexas.tacc.tapis.shared.utils.AuditUtils.AUDIT_ACTION;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.apache.commons.lang3.StringUtils;
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
     * @param targetPath - Absolute path of the affected file or directory
     * @param sourceSystem - Optional system containing the original file or directory. For txfr, copy, move actions
     * @param sourcePath - Absolute path of original file or directory.
     * @param reqTrackingId - Audit tracking Id received as part of the request.
     * @param impersonationId - use provided Tapis username instead of oboUser
     * @param data - Optional additional information associated with the event, as json
     */
    public AuditRecord(ResourceRequestUser rUser, String component, AUDIT_ACTION action, TapisSystem targetSystem,
                       String targetPath, TapisSystem sourceSystem, String sourcePath, String reqTrackingId,
                       String impersonationId, String data) {
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
        auditData.targetPath = targetPath;

        // TODO This way?
        auditData.sourceSystemId = (sourceSystem == null) ? AuditUtils.AUDIT_EMPTY : sourceSystem.getId();
        auditData.sourceSystemType = (sourceSystem == null) ? AuditUtils.AUDIT_EMPTY : sourceSystem.getSystemType().name();
        auditData.sourceHost = (sourceSystem == null) ? AuditUtils.AUDIT_EMPTY : sourceSystem.getHost();
        auditData.sourcePath = (sourceSystem == null || StringUtils.isBlank(sourcePath)) ? AuditUtils.AUDIT_EMPTY : sourcePath;
        // TODO or this way
        if (sourceSystem != null) {
            auditData.sourceSystemId = sourceSystem.getId();
            auditData.sourceSystemType = sourceSystem.getSystemType().name();
            auditData.sourceHost = sourceSystem.getId();
            auditData.sourcePath = (StringUtils.isBlank(sourcePath)) ? AuditUtils.AUDIT_EMPTY : sourcePath;
        } else {
            auditData.sourceSystemId = AuditUtils.AUDIT_EMPTY;
            auditData.sourceSystemType = AuditUtils.AUDIT_EMPTY;
            auditData.sourceHost = AuditUtils.AUDIT_EMPTY;
            auditData.sourcePath = AuditUtils.AUDIT_EMPTY;
        }

        auditData.trackingId = AuditUtils.TRACKING_PREFIX_FILES + UUID.randomUUID();
        auditData.parentTrackingId = StringUtils.isBlank(reqTrackingId) ? AuditUtils.AUDIT_EMPTY : reqTrackingId;
        auditData.data = StringUtils.isBlank(data) ? TapisConstants.EMPTY_JSON : data;
    }

    public AuditData getAuditData() { return  auditData; }
}
