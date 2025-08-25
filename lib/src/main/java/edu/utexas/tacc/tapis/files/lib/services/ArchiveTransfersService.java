package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCacheNoAuth;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.ArchiveTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.DAOTransactionContext;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.ArchiveTransfer;
import edu.utexas.tacc.tapis.files.lib.models.ArchiveTransferStatus;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
import edu.utexas.tacc.tapis.files.lib.models.TransferURI;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.utils.PathUtils;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static edu.utexas.tacc.tapis.files.lib.services.FileOpsService.SVCLIST_IMPERSONATE;

public class ArchiveTransfersService {
    private static final Logger log = LoggerFactory.getLogger(ArchiveTransfersService.class);

    public static int MAXIMUM_RETRIES = 3;

    @Inject
    private FileShareService shareService;
    @Inject
    private FilePermsService permsService;
    @Inject
    private SystemsCache systemsCache;
    @Inject
    private SystemsCacheNoAuth systemsCacheNoAuth;

    public ArchiveTransferResponse createArchiveTransfer(@NotNull ResourceRequestUser rUser, @NotNull final ArchiveTransfer archiveTransfer)
            throws ServiceException
    {
        String opName = "createArchiveTransfer";

        // set initialize fields
        archiveTransfer.setStatus(ArchiveTransferStatus.ACCEPTED);
        archiveTransfer.setRetriesRemaining(MAXIMUM_RETRIES);
        // Validate the request. Check that all Tapis systems exist and are enabled.
        // Check that transfer between system types is supported.
        validateRequest(rUser, archiveTransfer);

        // Persist the transfer task and associated parent tasks
        try
        {
            ArchiveTransfersDAO dao = new ArchiveTransfersDAO();
            ArchiveTransfer newTransfer = DAOTransactionContext.doInTransaction(context -> {
                return dao.insertArchiveTransfer(context, archiveTransfer);
            });
            return getResponseFromTransfer(newTransfer);
        }
        catch (DAOException e)
        {
            String msg = LibUtils.getMsgAuthR("FILES_ARCHIVE_TXFR_ERROR", rUser,
                    opName, "Archive Transfer could not be created", e);
            throw new ServiceException(msg, e);
        }
    }

    public ArchiveTransferResponse getArchiveTransfer(@NotNull ResourceRequestUser rUser, UUID uuid,
                                              boolean includePaths, String impersonationId)
            throws ServiceException, NotFoundException
    {
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
                String msg = LibUtils.getMsgAuthR("FILES_ARCHIVE_TXFR_ERROR", rUser,
                        opName, "Archive Transfer Not Found.  " + " uuid: " + uuid + " impersonationId: " + impersonationId);
                log.error(msg);
                throw new NotFoundException(msg);
            }

            // Do a final permission check based on calling user/tenant and task user/tenant
            isUserPermitted(rUser, archiveTransfer, oboOrImpersonatedUser, rUser.getOboTenantId(), opName);
            return getResponseFromTransfer(archiveTransfer);
        }
        catch (DAOException ex)
        {
            String msg = LibUtils.getMsgAuthR("FILES_ARCHIVE_TXFR_ERROR", rUser,
                    opName, ex.getMessage() + " uuid: " + uuid + " impersonationId: " + impersonationId);
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
        String msg = LibUtils.getMsgAuthR("FILES_ARCHIVE_TXFR_ERROR", rUser,
                opName, "User not authorized" + " uuid: " + archiveTransfer.getUuid());
        throw new ForbiddenException(msg);
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
            String msg = LibUtils.getMsgAuthR("FILES_ARCHIVE_TXFR_ERROR", rUser,
                    opName, "Only authorized services may impersonate a tapis user" +
                            " uuid: " + uuid + " impersonationId: " + impersonationId);
            throw new ForbiddenException(msg);
        }

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
        archiveTransferResponse.setSourceBaseUrl(archiveTransfer.getSourceBaseUrl());
        archiveTransferResponse.setDestinationBaseUrl(archiveTransfer.getDestinationBaseUrl());
        archiveTransferResponse.setRelativePaths(archiveTransfer.getRelativePaths());
        archiveTransferResponse.setStartTime(archiveTransfer.getStartTime());
        archiveTransferResponse.setEndTime(archiveTransfer.getEndTime());
        archiveTransferResponse.setArchiveBytesRead(archiveTransfer.getArchiveBytesRead());
        archiveTransferResponse.setFileBytesRead(archiveTransfer.getFileBytesRead());
        archiveTransferResponse.setErrorMessage(archiveTransfer.getErrorMessage());
        archiveTransferResponse.setSrcSharedCtxGrantor(archiveTransfer.getSrcSharedCtxGrantor());
        archiveTransferResponse.setDestSharedCtxGrantor(archiveTransfer.getDestSharedCtxGrantor());
        archiveTransferResponse.setArchiveType(archiveTransfer.getArchiveType());
        archiveTransferResponse.setRetriesRemaining(archiveTransfer.getRetriesRemaining());
        archiveTransferResponse.setNextRetry(archiveTransfer.getNextRetry());
        return archiveTransferResponse;
    }

    private void validateRequest(ResourceRequestUser rUser, ArchiveTransfer archiveTransfer) {
        final String opName = "validateRequest";
        TransferURI srcUri = new TransferURI(archiveTransfer.getSourceBaseUrl());
        TransferURI dstUri = new TransferURI(archiveTransfer.getDestinationBaseUrl());
        if((!srcUri.isTapisProtocol()) || (!dstUri.isTapisProtocol())) {
            String msg = LibUtils.getMsgAuthR("FILES_ARCHIVE_TXFR_ERROR", rUser,
                    opName, "Only the tapis protocol is supported by archive transfers" +
                            " srcUrl: " + srcUri + " dstUrl: " + dstUri);
            throw new BadRequestException(msg);
        }

        String srcSystemName = srcUri.getSystemId();
        String dstSystemName = srcUri.getSystemId();

        List<String> errorMsgs = new ArrayList<>();
        TapisSystem srcSystem = getEnabledSystem(rUser, opName + " (src)", srcSystemName, srcUri.getPath(),
                archiveTransfer.getSrcSharedCtxGrantor(), FileInfo.Permission.READ, errorMsgs);

        TapisSystem dstSystem = getEnabledSystem(rUser, opName + " (dst)", dstSystemName, dstUri.getPath(),
                archiveTransfer.getSrcSharedCtxGrantor(), FileInfo.Permission.MODIFY, errorMsgs);

        // at present, we know we only can support SSH source and destination, so only allow that.  If we later
        // support other options we can enhance this chaeck.
        if ((srcSystem != null && SystemTypeEnum.LINUX.equals(srcSystem.getSystemType())) &&
                (dstSystem == null || !SystemTypeEnum.LINUX.equals(dstSystem.getSystemType())))
        {
            String msg = LibUtils.getMsgAuthR("FILES_ARCHIVE_TXFR_ERROR", rUser,
                    opName, "Only Linux systems are supported currently by archive transfers" +
                            " srcUrl: " + srcUri + " dstUrl: " + dstUri);
        }

        if(!errorMsgs.isEmpty()) {
            StringBuilder errorBuilder = new StringBuilder();
            for(String errorMessage : errorMsgs) {
                errorBuilder.append("Error: ");
                errorBuilder.append(errorMessage);
                errorBuilder.append(".  ");
            }

            String msg = LibUtils.getMsgAuthR("FILES_ARCHIVE_TXFR_ERROR", rUser,
                opName, "Found errors in reauest:  " + errorBuilder.toString());
            throw new BadRequestException(msg);
        }

    }

    /**
     * Make sure a Tapis system exists and is enabled (with authorization)
     * For any not found or not enabled add a message to the list of error messages.
     * NOTE: Catch all exceptions, so we can collect and report as many errors as possible.
     */
    private TapisSystem getEnabledSystem(ResourceRequestUser rUser, String opName, String sysId, String pathStr,
                                         String sharedCtxGrantor, FileInfo.Permission perm, List<String> errMessages)
    {
        TapisSystem sys = null;
        // Get normalized path relative to system rootDir and protect against ../..
        String relPathStr = PathUtils.getRelativePath(pathStr).toString();
        try
        {
            // Fetch system with credentials including auth checks for system and path
            sys = LibUtils.getResolvedSysWithAuthCheck(rUser, shareService, systemsCache, systemsCacheNoAuth, permsService,
                    opName, sysId, relPathStr, perm, null, sharedCtxGrantor);
        }
        catch (Exception e) {
            errMessages.add(e.getMessage());
            return null;
        }

        return sys;
    }

}
