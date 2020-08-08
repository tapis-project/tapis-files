package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.ControlMessage;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import reactor.core.publisher.Flux;
import reactor.rabbitmq.AcknowledgableDelivery;

import javax.validation.constraints.NotNull;
import javax.ws.rs.NotFoundException;
import java.util.List;
import java.util.UUID;

public interface ITransfersService {

    void setParentQueue(@NotNull String parentQueue);
    void setChildQueue(@NotNull String childQueue);
    void setControlQueue(@NotNull String controlQueue);
    boolean isPermitted(@NotNull String username, @NotNull String tenantId, @NotNull UUID transferId) throws ServiceException;
    TransferTaskChild createTransferTaskChild(@NotNull TransferTaskChild task) throws ServiceException;
    TransferTask getTransferTask(@NotNull long taskId) throws ServiceException, NotFoundException;
    TransferTask getTransferTaskByUUID(@NotNull UUID taskUUID) throws ServiceException, NotFoundException;
    List<TransferTaskChild> getAllChildrenTasks(TransferTask task) throws ServiceException;
    void cancelTransfer(@NotNull TransferTask task) throws ServiceException, NotFoundException;
    List<TransferTask> getAllTransfersForUser(String tenantId, String username) throws ServiceException;

    TransferTask createTransfer(@NotNull String username, @NotNull String tenantId,
                                @NotNull String sourceSystemId, @NotNull String sourcePath,
                                @NotNull String destinationSystemId, @NotNull String destinationPath) throws ServiceException;

    Flux<ControlMessage> streamControlMessages();
    Flux<TransferTaskChild> processChildTasks(@NotNull Flux<AcknowledgableDelivery> messageStream);
    Flux<AcknowledgableDelivery> streamChildMessages();

    Flux<AcknowledgableDelivery> streamParentMessages();
    Flux<TransferTask> processParentTasks(Flux<AcknowledgableDelivery> messageStream);


}
