package edu.utexas.tacc.tapis.files.lib.services;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.ConnectionFactory;
import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
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
import edu.utexas.tacc.tapis.files.lib.transfers.ParentTaskFSM;
import edu.utexas.tacc.tapis.files.lib.transfers.ParentTaskReceiver;
import edu.utexas.tacc.tapis.files.lib.utils.SystemsClientFactory;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.*;

import javax.inject.Inject;
import javax.inject.Named;
import javax.validation.constraints.NotNull;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.ServerErrorException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

@Service
public class TransfersService implements ITransfersService {
    private static final Logger log = LoggerFactory.getLogger(TransfersService.class);
    private String PARENT_QUEUE = "tapis.files.transfers.parent";
    private String CHILD_QUEUE = "tapis.files.transfers.child";
    private static final int MAX_RETRIES = 5;
    private final Receiver receiver;
    private final Sender sender;

    private ParentTaskFSM fsm;
    private FileTransfersDAO dao;
    private SystemsClientFactory systemsClientFactory;
    private RemoteDataClientFactory remoteDataClientFactory;

    private static final ObjectMapper mapper = TapisObjectMapper.getMapper();

    @Inject
    public TransfersService(ParentTaskFSM fsm,
                            FileTransfersDAO dao,
                            SystemsClientFactory systemsClientFactory,
                            RemoteDataClientFactory remoteDataClientFactory) {
        ConnectionFactory connectionFactory = RabbitMQConnection.getInstance();
        ReceiverOptions receiverOptions = new ReceiverOptions()
            .connectionFactory(connectionFactory)
            .connectionSubscriptionScheduler(Schedulers.newElastic("receiver"));
        SenderOptions senderOptions = new SenderOptions()
            .connectionFactory(connectionFactory)
            .resourceManagementScheduler(Schedulers.newElastic("sender"));
        receiver = RabbitFlux.createReceiver(receiverOptions);
        sender = RabbitFlux.createSender(senderOptions);
        this.dao = dao;
        this.fsm = fsm;
        this.systemsClientFactory = systemsClientFactory;
        this.remoteDataClientFactory = remoteDataClientFactory;

        //TODO: Test out rabbitmq connection and auto shut down if not available?
    }

    public void setParentQueue(String name) {
        this.PARENT_QUEUE = name;
    }

    public boolean isPermitted(@NotNull String username, @NotNull String tenantId, @NotNull String transferTaskId) throws ServiceException {
        try {
            TransferTask task = dao.getTransferTask(transferTaskId);
            return task.getTenantId().equals(tenantId) && task.getUsername().equals(username);
        } catch (DAOException ex) {
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
            throw new ServiceException(ex.getMessage());
        }
    }

    public TransferTask createTransfer(@NotNull String username, @NotNull String tenantId,
                                       String sourceSystemId, String sourcePath,
                                       String destinationSystemId, String destinationPath) throws ServiceException {

        TransferTask task = new TransferTask(tenantId, username, sourceSystemId, sourcePath, destinationSystemId, destinationPath);

        try {
            // TODO: check if requesting user has access to both the source and dest systems, throw an error back if not
            task = dao.createTransferTask(task);
            this.publishParentTaskMessage(task);
            return task;
        } catch (DAOException ex) {
            throw new ServiceException(ex.getMessage());
        }
    }

    public TransferTaskChild createTransferTaskChild(@NotNull TransferTaskChild task) throws ServiceException {
        try {
            return dao.insertChildTask(task);
        } catch (DAOException ex) {
            throw new ServiceException(ex.getMessage());
        }
    }

    public void cancelTransfer(@NotNull TransferTask task) throws ServiceException, NotFoundException {
        try {
            task.setStatus(TransferTaskStatus.CANCELLED.name());
            dao.updateTransferTask(task);
        } catch (DAOException ex) {
            throw new ServiceException(ex.getMessage());
        }
    }

    private void publishParentTaskMessage(@NotNull TransferTask task) throws ServiceException {
        try {
            String m = mapper.writeValueAsString(task);
            OutboundMessage message = new OutboundMessage("", PARENT_QUEUE, m.getBytes(StandardCharsets.UTF_8));
            sender.send(Mono.just(message)).subscribe();
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new ServiceException(e.getMessage());
        }
    }

    public Flux<AcknowledgableDelivery> streamParentMessages() {
        ConsumeOptions options = new ConsumeOptions();
        options.qos(1000);
        Flux<AcknowledgableDelivery> parentMessageStream = receiver.consumeManualAck(PARENT_QUEUE, options);
        return parentMessageStream
            .delaySubscription(sender.declareQueue(QueueSpecification.queue(PARENT_QUEUE)));
    }

    public Flux<TransferTask> processParentTasks(Flux<AcknowledgableDelivery> messageStream) {
        return messageStream.groupBy((message) -> {
            try {
                return Mono.just(mapper.readValue(message.getBody(), TransferTask.class).getTenantId());
            } catch (IOException ex) {
                log.error("invalid message", ex);
                return Mono.empty();
            }
        })
        .map(Flux::parallel)
        .flatMap(group ->
            group.runOn(Schedulers.newBoundedElastic(8, 1, "pool"))
                .flatMap(m -> Mono.fromCallable(() -> {
                    TransferTask task = mapper.readValue(m.getBody(), TransferTask.class);
                    doStepOne(task);
                    m.ack();
                    return task;
                }).retry(MAX_RETRIES))
        );

    }

    public void doStepOne(TransferTask task) throws ServiceException {
        TSystem sourceSystem;
        IRemoteDataClient sourceClient;
        try {
            SystemsClient systemsClient = systemsClientFactory.getClient(task.getTenantId(), task.getUsername());
            sourceSystem = systemsClient.getSystemByName(task.getSourceSystemId(), null);
            sourceClient = remoteDataClientFactory.getRemoteDataClient(sourceSystem, task.getUsername());

            //TODO: Bulk inserts for both
            List<FileInfo> fileListing;
            fileListing = sourceClient.ls(task.getSourcePath());
            fileListing.forEach(f -> {
                TransferTaskChild child = new TransferTaskChild(task, f.getPath());
                try {
                    dao.insertChildTask(child);
                    publishChildMessage(child);
                } catch (ServiceException | DAOException ex) {
                    log.error(child.toString());
                }
            });
            task.setStatus(TransferTaskStatus.STAGED.name());
            dao.updateTransferTask(task);
        } catch (Exception e) {
            throw new ServiceException(e.getMessage());
        }
    }

    public List<TransferTaskChild> getAllChildrenTasks(TransferTask task) throws ServiceException {
        try {
            return dao.getAllChildren(task);
        } catch (DAOException e) {
            throw new ServiceException(e.getMessage());
        }
    }

    public List<TransferTask> getTransfersForUser(String tenantId, String username) throws ServiceException {
        try {
            return dao.getAllTransfersForUser(tenantId, username);
        } catch (DAOException ex) {
            throw new ServiceException(ex.getMessage());
        }
    }

    public void doTransfer(TransferTaskChild taskChild) throws ServiceException {

        try {
            //Step 1: Update task in DB to IN_PROGRESS and increment the retries on this particular task
            taskChild.setStatus(TransferTaskStatus.IN_PROGRESS.name());
            taskChild.setRetries(taskChild.getRetries() + 1);
            dao.updateTransferTaskChild(taskChild);
        } catch (DAOException ex) {
            throw new ServiceException(ex.getMessage());
        }
        //Step 2: Get clients for source / dest
        try {
            SystemsClient systemsClient = systemsClientFactory.getClient(taskChild.getTenantId(), taskChild.getUsername());
            TSystem sourceSystem = systemsClient.getSystemByName(taskChild.getSourceSystemId(), null);
            IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(sourceSystem, taskChild.getUsername());
            TSystem destSystem = systemsClient.getSystemByName(taskChild.getDestinationSystemId(), null);
            IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(destSystem, taskChild.getUsername());


        } catch (TapisClientException | IOException ex) {
            log.error(ex.getMessage(), ex);
            throw new ServiceException(ex.getMessage());
        }




        //Step 3: Stream the file contents to dest

        //Step 4: Update the parent task with bytes_transferred and check for completeness

    }

    private void publishChildMessage(TransferTaskChild childTask) throws ServiceException {
        try {
            String m = mapper.writeValueAsString(childTask);
            OutboundMessage message = new OutboundMessage("", CHILD_QUEUE, m.getBytes(StandardCharsets.UTF_8));
            sender.send(Flux.just(message)).subscribe();
        } catch (JsonProcessingException ex) {
            log.error(ex.getMessage(), ex);
            throw new ServiceException(ex.getMessage());
        }
    }
}
