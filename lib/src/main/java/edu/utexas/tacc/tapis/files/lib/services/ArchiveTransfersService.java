package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.dao.transfers.ArchiveTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.DAOTransactionContext;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.ArchiveTransfer;
import edu.utexas.tacc.tapis.files.lib.models.ArchiveTransferStatus;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotFoundException;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;

import static edu.utexas.tacc.tapis.files.lib.services.FileOpsService.SVCLIST_IMPERSONATE;

public class ArchiveTransfersService {
    private static final Logger log = LoggerFactory.getLogger(ArchiveTransfersService.class);
    public ArchiveTransferResponse createArchiveTransfer(@NotNull ResourceRequestUser rUser, @NotNull final ArchiveTransfer archiveTransfer)
            throws ServiceException
    {
        String opName = "createArchiveTransfer";

        // set initialize fields
        // TODO AXFER: Make a status object for this.
        archiveTransfer.setStatus(ArchiveTransferStatus.ACCEPTED);
        // Validate the request. Check that all Tapis systems exist and are enabled.
        // Check that transfer between system types is supported.
        validateRequest(rUser, archiveTransfer);

        // Persist the transfer task and associated parent tasks
        try
        {
            // TODO AXFER: Log message
            log.trace(LibUtils.getMsgAuthR("FILES_TXFR_PERSIST_TASK", rUser, archiveTransfer));
            ArchiveTransfersDAO dao = new ArchiveTransfersDAO();
            ArchiveTransfer newTransfer = DAOTransactionContext.doInTransaction(context -> {
                return dao.insertArchiveTransfer(context, archiveTransfer);
            });
            return getResponseFromTransfer(newTransfer);
        }
        catch (DAOException e)
        {
            // TODO AXFER: Error message
            String msg = LibUtils.getMsgAuthR("FILES_TXFR_SVC_ERR6", rUser, opName, e.getMessage());
            throw new ServiceException(msg, e);
        }
    }

    public ArchiveTransferResponse getArchiveTransfer(@NotNull ResourceRequestUser rUser, UUID uuid,
                                              boolean includePaths, String impersonationId)
            throws ServiceException, NotFoundException
    {
        // TODO AXFER: what the heck do I need to do with impersonationId?
        String opName = "getTransferTaskByUuid";
        // Check caller has permission to use impersonationId
        checkPermImpersonate(rUser, impersonationId, opName, uuid);

        // Certain services are allowed to impersonate an OBO user for the purposes of authorization
        String oboOrImpersonatedUser = StringUtils.isBlank(impersonationId) ? rUser.getOboUserId() : impersonationId;
        try
        {
            // Get the task, including summary info if requested.
            ArchiveTransfersDAO dao = new ArchiveTransfersDAO();
            ArchiveTransfer archiveTransfer = DAOTransactionContext.doInTransaction(context -> {
                return dao.getArchiveTransfer(context, uuid, false, includePaths);
            });
            if (archiveTransfer == null)
            {
                // TODO AXFER: need a real error message for archive transfers
                String msg = LibUtils.getMsgAuthR("FILES_TXFR_SVC_NOT_FOUND", rUser  ,opName, uuid, impersonationId);
                log.error(msg);
                throw new NotFoundException(msg);
            }
            // Do a final permission check based on calling user/tenant and task user/tenant
            isUserPermitted(rUser, archiveTransfer, oboOrImpersonatedUser, rUser.getOboTenantId(), opName);
            return getResponseFromTransfer(archiveTransfer);
        }
        catch (DAOException ex)
        {
            // TODO AXFER: need a real error message for archive transfers
            String msg = LibUtils.getMsgAuthR("FILES_TXFR_SVC_ERR3", rUser, opName, uuid, impersonationId, ex.getMessage());
            log.error(msg, ex);
            throw new ServiceException(msg, ex);
        }
    }

    /**
     * Check that user has permission to access and act on the task
     * Permitted only if task tenant+user match obo tenant+user
     * @param archiveTransfer - task to check
     * @param oboUser - user trying to act on the task
     */
    private void isUserPermitted(ResourceRequestUser rUser, ArchiveTransfer archiveTransfer, String oboUser, String oboTenant, String opName)
    {
        if (archiveTransfer.getTenantId().equals(oboTenant) && archiveTransfer.getUsername().equals(oboUser)) return;
        // TODO AXFER: check error message
        throw new ForbiddenException(LibUtils.getMsgAuthR("FILES_TASK_UNAUTH", rUser, oboTenant,
                oboUser, archiveTransfer.getTenantId(), archiveTransfer.getUsername(), archiveTransfer.getUuid(), opName));
    }

    /*
     * Check that caller is allowed to use impersonation
     * Permitted only for certain services.
     */
    private static void checkPermImpersonate(ResourceRequestUser rUser, String impersonationId, String opName, UUID uuid)
    {
        // If no impersonation then OK, return now.
        if (StringUtils.isBlank(impersonationId)) return;

        // If a service request the username will be the service name. E.g. systems, jobs, streams, etc
        String svcName = rUser.getJwtUserId();
        if (!rUser.isServiceRequest() || !SVCLIST_IMPERSONATE.contains(svcName))
        {
            // TODO AXFER: check error message
            String msg = LibUtils.getMsgAuthR("FILES_UNAUTH_IMPERSONATE_TXFR", rUser, opName,
                    uuid, impersonationId);
            throw new ForbiddenException(msg);
        }
        // TODO AXFER: check error message
        // An allowed service is impersonating, log it
        log.info(LibUtils.getMsgAuthR("FILES_AUTH_IMPERSONATE_TXFR", rUser, opName,
                uuid, impersonationId));
    }

    private ArchiveTransferResponse getResponseFromTransfer(ArchiveTransfer archiveTransfer) {
        ArchiveTransferResponse archiveTransferResponse = new ArchiveTransferResponse();
        archiveTransferResponse.setId(archiveTransfer.getId());
        archiveTransferResponse.setUuid(archiveTransfer.getUuid());
        archiveTransferResponse.setUsername(archiveTransfer.getUsername());
        archiveTransferResponse.setTenantId(archiveTransfer.getTenantId());
        archiveTransferResponse.setCreated(archiveTransfer.getCreated());
        archiveTransferResponse.setStatus(archiveTransfer.getStatus().name());
        archiveTransferResponse.setSourceBaseUrl(archiveTransferResponse.getSourceBaseUrl());
        archiveTransferResponse.setDestinationBaseUrl(archiveTransferResponse.getDestinationBaseUrl());
        archiveTransferResponse.setRelativePaths(archiveTransfer.getRelativePaths());
        archiveTransferResponse.setStartTime(archiveTransfer.getStartTime());
        archiveTransferResponse.setEndTime(archiveTransfer.getEndTime());
        archiveTransferResponse.setArchiveBytesRead(archiveTransfer.getArchiveBytesRead());
        archiveTransferResponse.setFileBytesRead(archiveTransfer.getFileBytesRead());
        archiveTransferResponse.setErrorMessage(archiveTransfer.getErrorMessage());
        archiveTransferResponse.setSrcSharedCtxGrantor(archiveTransfer.getSrcSharedCtxGrantor());
        archiveTransferResponse.setDestSharedCtxGrantor(archiveTransfer.getDestSharedCtxGrantor());
        return archiveTransferResponse;
    }

    private void validateRequest(ResourceRequestUser rUser, ArchiveTransfer archiveTransfer) {
        // TODO AXFER: Do some validation (see TransferService)
    }

}
