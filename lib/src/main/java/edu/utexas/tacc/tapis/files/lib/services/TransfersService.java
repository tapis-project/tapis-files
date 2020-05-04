package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.json.TapisObjectMapper;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import edu.utexas.tacc.tapis.files.lib.rabbit.RabbitMQConnection;
import edu.utexas.tacc.tapis.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import javax.ws.rs.NotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Service
public class TransfersService implements ITransfersService {
    private Logger log = LoggerFactory.getLogger(TransfersService.class);

    @Inject
    private FileTransfersDAO dao;

    @Inject
    private SystemsClient systemsClient;

    @Inject
    private RemoteDataClientFactory remoteDataClientFactory;

    private final String EXCHANGE_NAME = "tapis.files";
    private final String QUEUE_NAME = "tapis.files.transfers";
    private Connection connection;
    private Channel channel;
    private static final ObjectMapper mapper = TapisObjectMapper.getMapper();

    public TransfersService() throws ServiceException {
        try {
            connection = RabbitMQConnection.getInstance();
            channel = connection.createChannel();
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        } catch (IOException ex) {
            log.error(ex.getMessage());
            throw new ServiceException("Could not connect to RabbitMQ");
        }
    }

    public boolean isPermitted(@NotNull String username, @NotNull String tenantId, @NotNull String transferTaskId) throws ServiceException {
        try {
            TransferTask task = dao.getTransferTask(transferTaskId);
            return task.getTenantId().equals(tenantId) && task.getUsername().equals(username);
        } catch (DAOException ex) {
            log.error("ERROR", ex);
            throw new ServiceException(ex.getMessage());
        }
    }

    public TransferTask getTransferTask(@NotNull String taskUUID) throws ServiceException, NotFoundException {
        UUID taskId = UUID.fromString(taskUUID);
        return getTransferTask(taskId);
    }

    public TransferTask getTransferTask(@NotNull UUID taskUUID) throws ServiceException, NotFoundException {
        try {
            TransferTask task = dao.getTransferTask(taskUUID);
            if (task == null) {
                throw new NotFoundException("No transfer task with this UUID found.");
            }
            return task;
        } catch (DAOException ex) {
            log.error("ERROR: getTransferTask", ex);
            throw new ServiceException(ex.getMessage());
        }
    }


    public TransferTask createTransfer(@NotNull  String username, @NotNull String tenantId,
                                       String sourceSystemId, String sourcePath,
                                       String destinationSystemId, String destinationPath) throws ServiceException {

        TransferTask task = new TransferTask(tenantId, username, sourceSystemId, sourcePath, destinationSystemId, destinationPath);

        try {
            // TODO: check if requesting user has access to both the source and dest systems, throw an error back if not
            task = dao.createTransferTask(task);
            createTransferTaskChild(task, sourcePath);
            return task;
        } catch (DAOException ex) {
            log.error("ERROR: createTransfer", ex);
            throw new ServiceException(ex.getMessage());
        }
    }


    //TODO: Need to make this retryable since the listing might fail
    public void createTransferTaskChild(@NotNull TransferTask parentTask, @NotNull String sourcePath) throws ServiceException {
        TransferTaskChild transferTaskChild = new TransferTaskChild(parentTask, sourcePath);
        try {
            TSystem sourceSystem = systemsClient.getSystemByName(parentTask.getSourceSystemId());
            IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(sourceSystem, transferTaskChild.getUsername());
            // If its a dir, keep going down the tree
            if (sourcePath.endsWith("/")) {
                // For every item in the inital listing, create a childTask
                for(FileInfo item : sourceClient.ls(sourcePath)) {
                    createTransferTaskChild(parentTask, item.getPath());
                }
            } else {
                transferTaskChild = dao.createTransferTaskChild(parentTask, sourcePath);
                // Publish message for workers to pick up and actually do the transfer
                publishTransferTaskChildMessage(transferTaskChild);
            }
        } catch (DAOException ex) {
            log.error("ERROR: createTransfer", ex);
            throw new ServiceException(ex.getMessage());
        } catch (TapisClientException ex) {
            log.error("ERROR: createTransfer");
            throw new ServiceException(ex.getMessage());
        } catch (IOException ex) {
            log.error("ERROR: createTransfer", ex);
            throw new ServiceException("Remote client error");
        }

    }

    public void cancelTransfer(@NotNull TransferTask task) throws ServiceException, NotFoundException {
        try {
            task.setStatus(TransferTaskStatus.CANCELLED);
            dao.updateTransferTask(task);
        } catch (DAOException ex) {
            log.error("ERROR: cancelTransfer", ex);
            throw new ServiceException(ex.getMessage());
        }
    }

    public void publishTransferTaskChildMessage(@NotNull TransferTaskChild task) throws ServiceException {
        try {
            String message  = mapper.writeValueAsString(task);
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
        } catch (IOException ex) {
            log.error("ERROR: publishTransferTaskMessage", ex);
            throw new ServiceException(ex.getMessage());
        }

    }

    public void publishTransferTaskMessage(@NotNull TransferTask task) throws ServiceException {
        try {
            String message  = mapper.writeValueAsString(task);
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
        } catch (IOException ex) {
            log.error("ERROR: publishTransferTaskMessage", ex);
            throw new ServiceException(ex.getMessage());
        }

    }


}
