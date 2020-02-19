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
import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Service
public class TransfersService implements ITransfersService {
    private Logger log = LoggerFactory.getLogger(TransfersService.class);

    @Inject
    private FileTransfersDAO dao;

    @Inject
    private SystemsClient systemsClient;

    private final String EXCHANGE_NAME = "tapis.files";
    private final String QUEUE_NAME = "tapis.files.transfers";
    private Connection connection;
    private Channel channel;
    private static final ObjectMapper mapper = TapisObjectMapper.getMapper();

    public TransfersService() throws ServiceException {
        try {
            Connection conn = RabbitMQConnection.getInstance();
            connection = conn;
            channel = conn.createChannel();
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        } catch (IOException ex) {
            log.error(ex.getMessage());
            throw new ServiceException("Could not connect to RabbitMQ");
        }
    }

    public boolean isPermitted(String username, String tenantId, String transferTaskId) throws ServiceException {
        try {
            TransferTask task = dao.getTransferTask(transferTaskId);

            if (task.getTenantId() != tenantId || task.getUsername() != username) {
                return false;
            }
            return true;
        } catch (DAOException ex) {
            log.error("ERROR", ex);
            throw new ServiceException(ex.getMessage());
        }
    }


    public TransferTask createTransfer(String username, String tenantId,
                                       String sourceSystemId, String sourcePath,
                                       String destinationSystemId, String destinationPath) throws ServiceException {

        TransferTask task = new TransferTask(tenantId, username, sourceSystemId, sourcePath, destinationSystemId, destinationPath);

        // TODO: The remote clients could be cached to prevent thrashing on the systems service
        try {
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
        // TODO: The remote clients could be cached to prevent thrashing on the systems service
        try {
            TSystem sourceSystem = systemsClient.getSystemByName(parentTask.getSourceSystemId(), true, "ACCESS_TOKEN");
            IRemoteDataClient sourceClient = new RemoteDataClientFactory().getRemoteDataClient(sourceSystem);

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

    public void cancelTransfer(@NotNull TransferTask task) throws ServiceException {
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
