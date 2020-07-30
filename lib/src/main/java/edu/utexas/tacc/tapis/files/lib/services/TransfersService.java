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
import java.util.UUID;

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

//    private static final List<TransferTaskStatus> FINAL_STATES = {TransferTaskStatus.FAILED, TransferTaskStatus.CANCELLED}
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


    public boolean isPermitted(@NotNull String username, @NotNull String tenantId, @NotNull UUID transferTaskId) throws ServiceException {
        try {
            TransferTask task = dao.getTransferTaskByUUID(transferTaskId);
            return task.getTenantId().equals(tenantId) && task.getUsername().equals(username);
        } catch (DAOException ex) {
            throw new ServiceException(ex.getMessage());
        }
    }

    public List<TransferTaskChild> getAllChildrenTasks(TransferTask task) throws ServiceException {
        try {
            return dao.getAllChildren(task);
        } catch (DAOException e) {
            throw new ServiceException(e.getMessage(), e);
        }
    }

    public List<TransferTask> getAllTransfersForUser(String tenantId, String username) throws ServiceException {
        try {
            return dao.getAllTransfersForUser(tenantId, username);
        } catch (DAOException ex) {
            throw new ServiceException(ex.getMessage(), ex);
        }
    }

    public TransferTask getTransferTask(@NotNull long taskId) throws ServiceException, NotFoundException {
        try {
            TransferTask task = dao.getTransferTaskById(taskId);
            if (task == null) {
                throw new NotFoundException("No transfer task with this ID found.");
            }
            return task;
        } catch (DAOException ex) {
            throw new ServiceException(ex.getMessage(), ex);
        }
    }

    public TransferTask getTransferTaskByUUID(@NotNull UUID taskUUID) throws ServiceException, NotFoundException {
        try {
            TransferTask task = dao.getTransferTaskByUUID(taskUUID);
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
            // TODO: Do this in a single transaction
            TransferTask newTask = dao.createTransferTask(task);
            this.publishParentTaskMessage(newTask);
            return newTask;
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
            OutboundMessage message = new OutboundMessage("", PARENT_QUEUE, m.getBytes());
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

    public Flux<TransferTask> processParentTasks(Flux<AcknowledgableDelivery> messageStream) {
        return messageStream
            .groupBy(this::groupByTenant)
            .flatMap(group ->
                group.parallel()
                    .runOn(Schedulers.newParallel("pool"))
                    .flatMap(m -> this.processParentMessage(m).retry(MAX_RETRIES))
            );
    }


    private Mono<TransferTask> processParentMessage(AcknowledgableDelivery message) {
        return Mono.fromCallable(() -> {
            try {
                TransferTask task = mapper.readValue(message.getBody(), TransferTask.class);
                task = doParentListing(task);
                message.ack();
                return task;
            } catch (IOException | ServiceException ex) {
                message.nack(true);
                throw new ServiceException(ex.getMessage(), ex);
            }
        });

    }

    public TransferTask doParentListing(TransferTask parentTask) throws ServiceException {
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
            long totalBytes = 0;
            for (FileInfo f : fileListing) {
                totalBytes += f.getSize();
                TransferTaskChild child = new TransferTaskChild(finalParentTask, f);
                try {
                    child = dao.insertChildTask(child);
                    publishChildMessage(child);
                } catch (ServiceException | DAOException ex) {
                    log.error(child.toString());
                }
            }
            parentTask.setTotalBytes(totalBytes);
            parentTask.setStatus(TransferTaskStatus.STAGED.name());
            parentTask = dao.updateTransferTask(parentTask);
            return parentTask;
        } catch (Exception e) {
            throw new ServiceException(e.getMessage(), e);
        }
    }



    /**
     *
     *   Child task message processing
     *
     */

    public void publishChildMessage(TransferTaskChild childTask) throws ServiceException {
        try {
            String m = mapper.writeValueAsString(childTask);
            OutboundMessage message = new OutboundMessage("", CHILD_QUEUE, m.getBytes(StandardCharsets.UTF_8));
            sender.send(Flux.just(message)).subscribe();
        } catch (JsonProcessingException ex) {
            throw new ServiceException(ex.getMessage(), ex);
        }
    }

    private Mono<TransferTaskChild> deserializeChildMessage(AcknowledgableDelivery message) {
        try {
            TransferTaskChild child = mapper.readValue(message.getBody(), TransferTaskChild.class);
            return Mono.just(child);
        } catch (IOException ex) {
            // DO NOT requeue the message if it fails here!
            message.nack(false);
            return Mono.empty();
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

    public Flux<AcknowledgableDelivery> streamChildMessages() {
        ConsumeOptions options = new ConsumeOptions();
        options.qos(1000);
        Flux<AcknowledgableDelivery> childMessageStream = receiver.consumeManualAck(CHILD_QUEUE, options);
        return childMessageStream
            .delaySubscription(sender.declareQueue(QueueSpecification.queue(CHILD_QUEUE)));
    }


    public Flux<TransferTaskChild> processChildTasks(@NotNull Flux<AcknowledgableDelivery> messageStream) {
        // Deserialize the message so that we can pass that into the reactor chain.
        return messageStream
            .groupBy(this::groupByParentTask)
            .log()
            .map(group -> group.parallel().runOn(Schedulers.newParallel("childPool::"+group.key())))
            .flatMap(g->
                g.flatMap(m->deserializeChildMessage(m)
                    .flatMap(t1->Mono.fromCallable(()->doChildTransferChevronOne(t1)).retry(MAX_RETRIES).onErrorResume(e->doErrorChevronOne(m, e, t1)))
                    .flatMap(t2->Mono.fromCallable(()->doChildTransferChevronTwo(t2)).retry(MAX_RETRIES).onErrorResume(e->doErrorChevronOne(m, e, t2)))
                    .flatMap(t3->Mono.fromCallable(()->doChildTransferChevronThree(t3)).retry(MAX_RETRIES).onErrorResume(e->doErrorChevronOne(m, e, t3)))
                    .flatMap(t4 -> {
                        m.ack();
                        return Mono.just(t4);
                    })
                )
            );
    }

    private Mono<TransferTaskChild> doErrorChevronOne(AcknowledgableDelivery message, Throwable ex, TransferTaskChild task) {
        message.nack(false);
        log.error("ERROR: {}", task);
        //TODO: Update task status to FAILED for both child and parent
        log.error(ex.getMessage(), ex);
        return Mono.empty();
    }

    private Mono<Void> doErrorChevronTwo(AcknowledgableDelivery message, Throwable ex, TransferTaskChild task) {
        message.nack(false);
        //TODO: Update task status to FAILED for both child and parent
        log.error(ex.getMessage(), ex);
        return Mono.error(ex);
    }

    private Mono<Void> doErrorChevronThree(AcknowledgableDelivery message, Throwable ex, TransferTaskChild task) {
        message.nack(false);
        //TODO: Update task status to FAILED for both child and parent
        log.error(ex.getMessage(), ex);
        return Mono.error(ex);
    }



    /**
     * Step one: We update the task's status and the parent task if necessary
     * @param taskChild
     * @return
     */
    private TransferTaskChild doChildTransferChevronOne(@NotNull TransferTaskChild taskChild) throws ServiceException {
        log.info("***** DOING doChildTransferChevronOne ****");
        try {
            TransferTask parentTask = dao.getTransferTaskById(taskChild.getParentTaskId());
            if (parentTask.getStatus().equals(TransferTaskStatus.CANCELLED.name())) {
                return taskChild;
            }
            taskChild = dao.getChildTask(taskChild);
            taskChild.setStatus(TransferTaskStatus.IN_PROGRESS.name());
            dao.updateTransferTaskChild(taskChild);

            // If the parent task has not been set to IN_PROGRESS do it here.
            if (!parentTask.getStatus().equals(TransferTaskStatus.IN_PROGRESS.name())) {
                parentTask.setStatus(TransferTaskStatus.IN_PROGRESS.name());
                dao.updateTransferTask(parentTask);
            }
            return taskChild;
        } catch (DAOException ex) {
            throw new ServiceException(ex.getMessage(), ex);
        }

    }

    private  TransferTaskChild doChildTransferChevronTwo(TransferTaskChild taskChild) throws ServiceException {
        log.info("***** DOING doChildTransferChevronTwo ****");
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
        } catch (TapisClientException | IOException | ServiceException ex) {
            log.error(ex.getMessage(), ex);
            throw new ServiceException(ex.getMessage(), ex);
        }

        //Step 3: Stream the file contents to dest
        try {
            InputStream sourceStream = sourceClient.getStream(taskChild.getSourcePath());
            destClient.insert(taskChild.getDestinationPath(), sourceStream);
        } catch (IOException | NotFoundException ex) {
            String msg = String.format("Error transferring %s", taskChild.toString());
            throw new ServiceException(ex.getMessage(), ex);

        }
        return taskChild;
    }

    /**
     * Mark the child as COMPLETE and check if the parent task is complete.
     * @param taskChild
     * @return
     */
    private TransferTaskChild doChildTransferChevronThree(@NotNull TransferTaskChild taskChild) throws ServiceException {
        log.info("***** DOING doChildTransferChevronThree ****");
        try {
            TransferTask parentTask = dao.getTransferTaskById(taskChild.getParentTaskId());
            taskChild.setStatus(TransferTaskStatus.COMPLETED.name());
            taskChild = dao.updateTransferTaskChild(taskChild);
            dao.updateTransferTaskBytesTransferred(parentTask.getId(), taskChild.getTotalBytes());
            long incompleteCount = dao.getIncompleteChildrenCount(taskChild.getParentTaskId());
            if (incompleteCount == 0) {
                parentTask.setStatus(TransferTaskStatus.COMPLETED.name());
                dao.updateTransferTask(parentTask);
            }
            return taskChild;
        } catch (DAOException ex) {
            String msg = String.format("Error updating child task %s", taskChild.toString());
            throw new ServiceException(ex.getMessage(), ex);
        }
    }


}
