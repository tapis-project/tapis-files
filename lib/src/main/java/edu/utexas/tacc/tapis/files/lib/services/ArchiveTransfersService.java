package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.dao.transfers.ArchiveTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.DAOTransactionContext;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.ArchiveTransfer;
import edu.utexas.tacc.tapis.files.lib.models.ArchiveTransferStatus;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;

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
        archiveTransferResponse.setBytesTransferred(archiveTransfer.getBytesTransferred());
        archiveTransferResponse.setErrorMessage(archiveTransfer.getErrorMessage());
        archiveTransferResponse.setSrcSharedCtxGrantor(archiveTransfer.getSrcSharedCtxGrantor());
        archiveTransferResponse.setDestSharedCtxGrantor(archiveTransfer.getDestSharedCtxGrantor());
        return archiveTransferResponse;
    }

    private void validateRequest(ResourceRequestUser rUser, ArchiveTransfer archiveTransfer) {
        // TODO AXFER: Do some validation (see TransferService)
    }

}
