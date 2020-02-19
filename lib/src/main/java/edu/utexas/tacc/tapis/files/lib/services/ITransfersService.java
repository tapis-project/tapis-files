package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;

import javax.validation.constraints.NotNull;

public interface ITransfersService {

    boolean isPermitted(@NotNull String username, @NotNull String tenantId, @NotNull String transferId) throws ServiceException;

    void createTransferTaskChild(@NotNull TransferTask parentTask, @NotNull String sourcePath) throws ServiceException;

    void cancelTransfer(@NotNull TransferTask task) throws ServiceException;

    TransferTask createTransfer(String username, String tenantId,
                                String sourceSystemId, String sourcePath,
                                String destinationSystemId, String destinationPath) throws ServiceException;

    void publishTransferTaskChildMessage(@NotNull TransferTaskChild task) throws ServiceException;

    void publishTransferTaskMessage(@NotNull TransferTask task) throws ServiceException;
}
