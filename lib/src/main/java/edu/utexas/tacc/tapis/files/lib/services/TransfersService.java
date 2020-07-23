package edu.utexas.tacc.tapis.files.lib.services;

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
import javax.validation.constraints.NotNull;
import javax.ws.rs.NotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

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
    public void setChildQueue(String name) {
        this.CHILD_QUEUE = name;
    }


    public boolean isPermitted(@NotNull String username, @NotNull String tenantId, @NotNull long transferTaskId) throws ServiceException {
        try {
            TransferTask task = dao.getTransferTaskById(transferTaskId);
            return task.getTenantId().equals(tenantId) && task.getUsername().equals(username);
        } catch (DAOException ex) {
            throw new ServiceException(ex.getMessage());
        }
    }


    public TransferTask getTransferTask(@NotNull long taskId) throws ServiceException, NotFoundException {
        try {
            TransferTask task = dao.getTransferTaskById(taskId);
            if (task == null) {
                throw new NotFoundException("No transfer task with this UUID found.");
            }
            return task;
        } catch (DAOException ex) {
            throw new ServiceException(ex.getMessage(), ex);
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
            throw new ServiceException(ex.getMessage(), ex);
        }
    }

    public TransferTaskChild createTransferTaskChild(@NotNull TransferTaskChild task) throws ServiceException {
        try {
            return dao.insertChildTask(task);
        } catch (DAOException ex) {
            throw new ServiceException(ex.getMessage(), ex);
        }
    }

    public void cancelTransfer(@NotNull TransferTask task) throws ServiceException, NotFoundException {
        try {
            task.setStatus(TransferTaskStatus.CANCELLED.name());
            dao.updateTransferTask(task);
        } catch (DAOException ex) {
            throw new ServiceException(ex.getMessage(), ex);
        }
    }

    public void deleteQueue(String qName) {
        sender.delete(QueueSpecification.queue(qName))
            .subscribe();
    }

    private void publishParentTaskMessage(@NotNull TransferTask task) throws ServiceException {
        try {
            String m = mapper.writeValueAsString(task);
            OutboundMessage message = new OutboundMessage("", PARENT_QUEUE, m.getBytes(StandardCharsets.UTF_8));
            Flux<OutboundMessageResult> confirms = sender.sendWithPublishConfirms(Mono.just(message));
            sender.declareQueue(QueueSpecification.queue(PARENT_QUEUE))
                .thenMany(confirms)
                .subscribe();
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

    private Mono<String> groupByTenant(AcknowledgableDelivery message) {
        try {
            return Mono.just(mapper.readValue(message.getBody(), TransferTask.class).getTenantId());
        } catch (IOException ex) {
            log.error("invalid message", ex);
            return Mono.error(ex);
        }
    }

    private Mono<Long> groupByParentTask(AcknowledgableDelivery message) {
        try {
            return Mono.just(mapper.readValue(message.getBody(), TransferTaskChild.class).getParentTaskId());
        } catch (IOException ex) {
            log.error("invalid message", ex);
            return Mono.error(ex);
        }
    }

    public Flux<TransferTaskChild> processChildTasks(Flux<AcknowledgableDelivery> messageStream) {
        return messageStream
            .groupBy(this::groupByParentTask)
            .flatMap(group ->
                group.parallel()
                    .runOn(Schedulers.newParallel("childPool"))
                    .flatMap(this::processChildMessage)
            );
     }

    private Mono<TransferTaskChild> processChildMessage(AcknowledgableDelivery message) {
        return Mono.fromCallable( ()-> {
            try {
                TransferTaskChild child = mapper.readValue(message.getBody(), TransferTaskChild.class);
                child = doTransfer(child);
                message.ack();
                return child;
            } catch (IOException | ServiceException ex) {
                message.nack(true);
                throw new ServiceException(ex.getMessage(), ex);
            }
        })
            .retry(MAX_RETRIES);
    }



    public Flux<TransferTask> processParentTasks(Flux<AcknowledgableDelivery> messageStream) {
         return messageStream
            .groupBy(this::groupByTenant)
            .flatMap(group ->
                group.parallel()
                    .runOn(Schedulers.newParallel("pool"))
                    .flatMap(this::processParentMessage)
            );
    }



    private Mono<TransferTask> processParentMessage(AcknowledgableDelivery message) {
        return Mono.fromCallable( ()-> {
            try {
                TransferTask task = mapper.readValue(message.getBody(), TransferTask.class);
                task = doStepOne(task);
                message.ack();
                return task;
            } catch (IOException | ServiceException ex) {
                message.nack(true);
                throw new ServiceException(ex.getMessage(), ex);
            }
        })
            .retry(MAX_RETRIES);

    }

    public TransferTask doStepOne(TransferTask parentTask) throws ServiceException {
        TSystem sourceSystem;
        IRemoteDataClient sourceClient;
        // Has to be final for lambda below
        final TransferTask finalParentTask = parentTask;
        try {
            SystemsClient systemsClient = systemsClientFactory.getClient(parentTask.getTenantId(), parentTask.getUsername());
            sourceSystem = systemsClient.getSystemByName(parentTask.getSourceSystemId(), null);
            sourceClient = remoteDataClientFactory.getRemoteDataClient(sourceSystem, parentTask.getUsername());

            //TODO: Bulk inserts for both
            List<FileInfo> fileListing;
            fileListing = sourceClient.ls(parentTask.getSourcePath());
            fileListing.forEach(f -> {
                TransferTaskChild child = new TransferTaskChild(finalParentTask, f.getPath());
                try {
                    dao.insertChildTask(child);
                    publishChildMessage(child);
                } catch (ServiceException | DAOException ex) {
                    log.error(child.toString());
                }
            });
            parentTask.setStatus(TransferTaskStatus.STAGED.name());
            parentTask = dao.updateTransferTask(parentTask);
            return parentTask;
        } catch (Exception e) {
            throw new ServiceException(e.getMessage(), e);
        }
    }

    public List<TransferTaskChild> getAllChildrenTasks(TransferTask task) throws ServiceException {
        try {
            return dao.getAllChildren(task);
        } catch (DAOException e) {
            throw new ServiceException(e.getMessage(), e);
        }
    }

    public List<TransferTask> getTransfersForUser(String tenantId, String username) throws ServiceException {
        try {
            return dao.getAllTransfersForUser(tenantId, username);
        } catch (DAOException ex) {
            throw new ServiceException(ex.getMessage(), ex);
        }
    }

    public TransferTaskChild doTransfer(TransferTaskChild taskChild) throws ServiceException {
        TSystem sourceSystem;
        TSystem destSystem;
        IRemoteDataClient sourceClient;
        IRemoteDataClient destClient;

        //Step 1: Update task in DB to IN_PROGRESS and increment the retries on this particular task
        try {
            taskChild.setStatus(TransferTaskStatus.IN_PROGRESS.name());
            taskChild.setRetries(taskChild.getRetries() + 1);
            taskChild = dao.updateTransferTaskChild(taskChild);
        } catch (DAOException ex) {
            throw new ServiceException(ex.getMessage(), ex);
        }
        //Step 2: Get clients for source / dest
        try {
            SystemsClient systemsClient = systemsClientFactory.getClient(taskChild.getTenantId(), taskChild.getUsername());
            sourceSystem = systemsClient.getSystemByName(taskChild.getSourceSystemId(), null);
            sourceClient = remoteDataClientFactory.getRemoteDataClient(sourceSystem, taskChild.getUsername());
            destSystem = systemsClient.getSystemByName(taskChild.getDestinationSystemId(), null);
            destClient = remoteDataClientFactory.getRemoteDataClient(destSystem, taskChild.getUsername());


        } catch (TapisClientException | IOException ex) {
            log.error(ex.getMessage(), ex);
            throw new ServiceException(ex.getMessage(), ex);
        }

        //Step 3: Stream the file contents to dest
        try {
            InputStream sourceStream = sourceClient.getStream(taskChild.getSourcePath());
            destClient.insert(taskChild.getDestinationPath(), sourceStream);
        } catch (IOException ex) {
            String msg = String.format("Error transferring %s", taskChild.toString());
            throw new ServiceException(msg, ex);
        }

        //Step 4: Update the parent task with bytes_transferred and check for completeness
        try {
            TransferTask parentTask = dao.getTransferTaskById(taskChild.getParentTaskId());
            taskChild.setStatus(TransferTaskStatus.COMPLETED.name());
            taskChild = dao.updateTransferTaskChild(taskChild);
            dao.updateTransferTaskBytesTransferred(parentTask.getId(), taskChild.getTotalBytes());
            long incompleteCount = dao.getUncompleteChildrenCount(taskChild.getParentTaskId());
            if (incompleteCount == 0) {
                parentTask.setStatus(TransferTaskStatus.COMPLETED.name());
                dao.updateTransferTask(parentTask);
            }
        } catch (DAOException ex) {
            String msg = String.format("Error updating child task %s", taskChild.toString());
            throw new ServiceException(msg, ex);
        }
        return taskChild;
    }

    private void publishChildMessage(TransferTaskChild childTask) throws ServiceException {
        try {
            String m = mapper.writeValueAsString(childTask);
            OutboundMessage message = new OutboundMessage("", CHILD_QUEUE, m.getBytes(StandardCharsets.UTF_8));
            sender.send(Flux.just(message)).subscribe();
        } catch (JsonProcessingException ex) {
            throw new ServiceException(ex.getMessage(), ex);
        }
    }
}
