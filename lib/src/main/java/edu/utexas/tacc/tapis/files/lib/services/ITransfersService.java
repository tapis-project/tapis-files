package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;

import javax.validation.constraints.NotNull;
import javax.ws.rs.NotFoundException;
import java.util.UUID;

public interface ITransfersService {

    TransferTask getTransferTask(long taskId) throws ServiceException, NotFoundException;
    void setParentQueue(String parentQueue);
    boolean isPermitted(@NotNull String username, @NotNull String tenantId, @NotNull UUID transferId) throws ServiceException;
    TransferTaskChild createTransferTaskChild(@NotNull TransferTaskChild task) throws ServiceException;

    void cancelTransfer(@NotNull TransferTask task) throws ServiceException, NotFoundException;

    TransferTask createTransfer(String username, String tenantId,
                                String sourceSystemId, String sourcePath,
                                String destinationSystemId, String destinationPath) throws ServiceException;

}
