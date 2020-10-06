package edu.utexas.tacc.tapis.files.lib.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP.Queue.DeleteOk;
import com.rabbitmq.client.ConnectionFactory;
import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.json.TapisObjectMapper;
import edu.utexas.tacc.tapis.files.lib.models.*;
import edu.utexas.tacc.tapis.files.lib.rabbit.RabbitMQConnection;
import edu.utexas.tacc.tapis.files.lib.transfers.ObservableInputStream;
import edu.utexas.tacc.tapis.files.lib.transfers.ParentTaskFSM;
import edu.utexas.tacc.tapis.files.lib.utils.SystemsClientFactory;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.*;

import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import javax.ws.rs.NotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
public class TransfersService implements ITransfersService {
    private static final Logger log = LoggerFactory.getLogger(TransfersService.class);
    private String PARENT_QUEUE = "tapis.files.transfers.parent";
    private String CHILD_QUEUE = "tapis.files.transfers.child";
    private String CONTROL_QUEUE = "tapis.files.transfers.control";
    private static final int MAX_RETRIES = 5;
    private final Receiver receiver;
    private final Sender sender;

    private final ParentTaskFSM fsm;
    private final FileTransfersDAO dao;
    private final SystemsClientFactory systemsClientFactory;
    private final RemoteDataClientFactory remoteDataClientFactory;
    private final NotificationsService notificationsService;

    private static final TransferTaskStatus[] FINAL_STATES = new TransferTaskStatus[]{
        TransferTaskStatus.FAILED,
        TransferTaskStatus.CANCELLED,
        TransferTaskStatus.COMPLETED};
    private static final ObjectMapper mapper = TapisObjectMapper.getMapper();

    @Inject
    public TransfersService(ParentTaskFSM fsm,
                            FileTransfersDAO dao,
                            SystemsClientFactory systemsClientFactory,
                            RemoteDataClientFactory remoteDataClientFactory,
                            NotificationsService notificationsService) {
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
        this.notificationsService = notificationsService;

        //TODO: Test out rabbitmq connection and auto shut down if not available?
    }

    public void setParentQueue(String name) {
        this.PARENT_QUEUE = name;
    }

    public void setChildQueue(String name) {
        this.CHILD_QUEUE = name;
    }

    public void setControlQueue(String name) {
        this.CONTROL_QUEUE = name;
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

    public Mono<DeleteOk> deleteQueue(String qName) {
        return sender.delete(QueueSpecification.queue(qName));
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

    private Mono<TransferTask> deserializeParentMessage(AcknowledgableDelivery message) {
        try {
            TransferTask child = mapper.readValue(message.getBody(), TransferTask.class);
            return Mono.just(child);
        } catch (IOException ex) {
            // DO NOT requeue the message if it fails here!
            message.nack(false);
            return Mono.empty();
        }
    }

    private String groupByTenant(AcknowledgableDelivery message) throws ServiceException {
        try {
            return mapper.readValue(message.getBody(), TransferTask.class).getTenantId();
        } catch (IOException ex) {
            log.error("invalid message", ex);
            throw new ServiceException("Something is terribly wrong, could not get a tenant???", ex);
        }
    }




    public Flux<AcknowledgableDelivery> streamParentMessages() {
        ConsumeOptions options = new ConsumeOptions();
        options.qos(1000);
        Flux<AcknowledgableDelivery> childMessageStream = receiver.consumeManualAck(PARENT_QUEUE, options);
        return childMessageStream
            .delaySubscription(sender.declareQueue(QueueSpecification.queue(PARENT_QUEUE)));
    }

    public Flux<TransferTask> processParentTasks(Flux<AcknowledgableDelivery> messageStream) {
        return messageStream
            .groupBy(m->{
                try {
                    return groupByTenant(m);
                } catch (ServiceException ex) {
                    return Mono.empty();
                }
            })
            .flatMap(group ->
                group
                    .publishOn(Schedulers.newBoundedElastic(10, 1, "ParentPool:" + group.key()))
                    .flatMap(m->
                        deserializeParentMessage(m)
                        .flatMap(t1->Mono.fromCallable(()-> doParentChevronOne(t1)).retry(MAX_RETRIES).onErrorResume(e -> doErrorParentChevronOne(m, e, t1)))
                        .flatMap(t2->{
                            m.ack();
                            return Mono.just(t2);
                        })
                    )
            );
    }

    private Mono<TransferTask> doErrorParentChevronOne(AcknowledgableDelivery m, Throwable e, TransferTask t) {
        //TODO: Add task failure, set status etc
        log.error("Error callback", e);
        m.nack(false);
        return Mono.empty();
    }

    private TransferTask doParentChevronOne(TransferTask parentTask) throws ServiceException {
        log.info("***** DOING doParentChevronOne ****");
        log.info(parentTask.toString());
        TSystem sourceSystem;
        IRemoteDataClient sourceClient;
        // Has to be final for lambda below
        final TransferTask finalParentTask = parentTask;
        try {
            SystemsClient systemsClient = systemsClientFactory.getClient(parentTask.getTenantId(), parentTask.getUsername());
            sourceSystem = systemsClient.getSystemByName(parentTask.getSourceSystemId(), null);
            sourceClient = remoteDataClientFactory.getRemoteDataClient(sourceSystem, parentTask.getUsername());

            //TODO: Retries will break this, should delete anything in the DB if it is a retry?
            List<FileInfo> fileListing;
            fileListing = sourceClient.ls(parentTask.getSourcePath());
            List<TransferTaskChild> children = new ArrayList<>();
            long totalBytes = 0;
            for (FileInfo f : fileListing) {
                totalBytes += f.getSize();
                TransferTaskChild child = new TransferTaskChild(finalParentTask, f);
                children.add(child);
            }
            parentTask.setTotalBytes(totalBytes);
            parentTask.setStatus(TransferTaskStatus.STAGED.name());
            parentTask = dao.updateTransferTask(parentTask);
            dao.bulkInsertChildTasks(children);
            children = dao.getAllChildren(parentTask);
            publishBulkChildMessages(children);
            String noteMessage = String.format("Transfer %s staged", parentTask.getUuid());
            notificationsService.sendNotification(parentTask.getTenantId(), parentTask.getUsername(), noteMessage);
            return parentTask;
        } catch (Exception e) {
            log.error("Error processing {}", parentTask);
            throw new ServiceException(e.getMessage(), e);
        }
    }


    /**
     * Child task message processing
     */

    public void publishBulkChildMessages(List<TransferTaskChild> children) {
            Flux<OutboundMessageResult> messages = sender.sendWithPublishConfirms(
                Flux.fromIterable(children)
                    .flatMap(task->{
                        try {
                            String m = mapper.writeValueAsString(task);
                            return Flux.just(new OutboundMessage("", CHILD_QUEUE, m.getBytes(StandardCharsets.UTF_8)));
                        } catch (JsonProcessingException e) {
                           return Flux.empty();
                        }
                    })
            );

            sender.declareQueue(QueueSpecification.queue(CHILD_QUEUE))
                .thenMany(messages)
                .subscribe();
    }

    public void publishChildMessage(TransferTaskChild childTask) throws ServiceException {
        try {
            String m = mapper.writeValueAsString(childTask);
            OutboundMessage message = new OutboundMessage("", CHILD_QUEUE, m.getBytes(StandardCharsets.UTF_8));

            sender.declareQueue(QueueSpecification.queue(CHILD_QUEUE))
                .subscribe();
            sender.send(Mono.just(message))
                .subscribe();
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

    private Long groupByParentTask(AcknowledgableDelivery message) throws ServiceException {
        try {
            return mapper.readValue(message.getBody(), TransferTaskChild.class).getParentTaskId();
        } catch (IOException ex) {
            log.error("invalid message", ex);
            throw new ServiceException("Could not decipher this message, something is off!", ex);
        }
    }

    public Flux<AcknowledgableDelivery> streamChildMessages() {
        ConsumeOptions options = new ConsumeOptions();
        options.qos(1000);
        Flux<AcknowledgableDelivery> childMessageStream = receiver.consumeManualAck(CHILD_QUEUE, options);
        return childMessageStream
            .delaySubscription(sender.declareQueue(QueueSpecification.queue(CHILD_QUEUE)));
    }


    /**
     * This is the main processing workflow. Starting with the raw message stream created by streamChildMessages(),
     * we walk through the transfer process. A Flux<TransferTaskChild> is returned and can be subscribed to later
     * for further processing / notifications / logging
     * @param messageStream
     * @return
     */
    public Flux<TransferTaskChild> processChildTasks(@NotNull Flux<AcknowledgableDelivery> messageStream) {
        // Deserialize the message so that we can pass that into the reactor chain.
        return messageStream
            .log()
            .groupBy( m-> {
                try {
                    return groupByParentTask(m);
                } catch (ServiceException ex) {
                    return Mono.empty();
                }
            })
            .flatMap(group-> {
                Scheduler scheduler = Schedulers.newBoundedElastic(6,1,"ChildPool:"+group.key());
                return group
                    .publishOn(scheduler)
                    .flatMap(
                        //Wwe need the message in scope so we can ack/nack it later
                        m -> deserializeChildMessage(m)
                            .flatMap(t1 -> Mono.fromCallable(() -> chevronOne(t1))
                                .retryBackoff(MAX_RETRIES, Duration.ofSeconds(1), Duration.ofSeconds(60), scheduler)
                                .onErrorResume(e -> doErrorChevronOne(m, e, t1))
                            )
                            .flatMap(t2 -> Mono.fromCallable(() -> chevronTwo(t2))
                                .retryBackoff(MAX_RETRIES, Duration.ofSeconds(1), Duration.ofSeconds(60), scheduler)
                                .onErrorResume(e -> doErrorChevronOne(m, e, t2))
                            )
                            .flatMap(t3 -> Mono.fromCallable(() -> chevronThree(t3))
                                .retryBackoff(MAX_RETRIES, Duration.ofSeconds(1), Duration.ofSeconds(60), scheduler)
                                .onErrorResume(e -> doErrorChevronOne(m, e, t3))
                            )
                            .flatMap(t4 -> Mono.fromCallable(() -> chevronFour(t4))
                                .retryBackoff(MAX_RETRIES, Duration.ofSeconds(1), Duration.ofSeconds(60), scheduler)
                                .onErrorResume(e -> doErrorChevronOne(m, e, t4))
                            )
                            .flatMap(t5 -> {
                                m.ack();
                                return Mono.just(t5);
                            })
                          .flatMap(t6->Mono.fromCallable(()->chevronFive(t6, scheduler)))

                    );
            });
    }

    /**
     *
     * @param message Message from rabbitmq
     * @param ex Exception that was thrown
     * @param task the Child task that failed
     * @return
     */
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
     *
     * @param taskChild
     * @return
     */
    private TransferTaskChild chevronOne(@NotNull TransferTaskChild taskChild) throws ServiceException {
        log.info("***** DOING chevronOne ****");
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
            String noteMessage = String.format("Transfer in progress for %s", taskChild.getSourcePath());
            notificationsService.sendNotification(taskChild.getTenantId(), taskChild.getUsername(), noteMessage);
            return taskChild;
        } catch (DAOException ex) {
            throw new ServiceException(ex.getMessage(), ex);
        }

    }


    /**
     * Chevron 2: The meat of the operation. We get remote clients for source and dest and send bytes
     * @param taskChild
     * @return
     * @throws ServiceException
     */
    private TransferTaskChild chevronTwo(TransferTaskChild taskChild) throws ServiceException {
        log.info("***** DOING chevronTwo ****");
        log.info(taskChild.toString());
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
            InputStream sourceStream =  sourceClient.getStream(taskChild.getSourcePath());
            ObservableInputStream observableInputStream = new ObservableInputStream(sourceStream);
            // Observe the progress event stream
            observableInputStream.getEventStream()
                .window(Duration.ofMillis(1000))
                .flatMap(window->window.takeLast(1))
                .publishOn(Schedulers.elastic())
                .flatMap(this::updateProgress)
                .subscribe();
            destClient.insert(taskChild.getDestinationPath(), observableInputStream);

        } catch (IOException | NotFoundException ex) {
            String msg = String.format("Error transferring %s", taskChild.toString());
            throw new ServiceException(msg, ex);

        }
        return taskChild;
    }

    private Mono<Long> updateProgress(Long aLong) {
        log.info(aLong.toString());
        return Mono.just(aLong);
    }

    /**
     * Book keeping and cleanup. Mark the child as COMPLETE and check if the parent task is complete.
     *
     * @param taskChild
     * @return
     */
    private TransferTaskChild chevronThree(@NotNull TransferTaskChild taskChild) throws ServiceException {
        log.info("***** DOING chevronThree **** {}", taskChild);
        try {
            TransferTask parentTask = dao.getTransferTaskById(taskChild.getParentTaskId());
            taskChild.setStatus(TransferTaskStatus.COMPLETED.name());
            taskChild = dao.updateTransferTaskChild(taskChild);
            parentTask = dao.updateTransferTaskBytesTransferred(parentTask.getId(), taskChild.getTotalBytes());
            String noteMessage = String.format("Transfer completed for %s", taskChild.getSourcePath());
            notificationsService.sendNotification(taskChild.getTenantId(), taskChild.getUsername(), noteMessage);
            return taskChild;
        } catch (DAOException ex) {
            String msg = String.format("Error updating child task %s", taskChild.toString());
            throw new ServiceException(msg, ex);
        }
    }

    private TransferTaskChild chevronFour(@NotNull TransferTaskChild taskChild) throws ServiceException {
        log.info("***** DOING chevronFour **** {}", taskChild);
        try {
            TransferTask parentTask = dao.getTransferTaskById(taskChild.getParentTaskId());
            // Check to see if all the children are complete. If so, update the parent task
            // TODO: Race condition
            if (!parentTask.getStatus().equals(TransferTaskStatus.COMPLETED.name())) {
                long incompleteCount = dao.getIncompleteChildrenCount(taskChild.getParentTaskId());
                if (incompleteCount == 0) {
                    parentTask.setStatus(TransferTaskStatus.COMPLETED.name());
                    dao.updateTransferTask(parentTask);
                    String noteMessage = String.format("Transfer %s complete", parentTask.getUuid());
                    notificationsService.sendNotification(taskChild.getTenantId(), taskChild.getUsername(), noteMessage);
                }
            }
            return taskChild;
        } catch (DAOException ex) {
            String msg = String.format("Error updating child task %s", taskChild.toString());
            throw new ServiceException(msg, ex);
        }
    }

    private TransferTaskChild chevronFive(@NotNull TransferTaskChild taskChild, Scheduler scheduler) throws ServiceException {
        log.info("***** DOING chevronFive **** {}", taskChild);
        try {
            TransferTask parentTask = dao.getTransferTaskById(taskChild.getParentTaskId());
            if (parentTask.getStatus().equals(TransferTaskStatus.COMPLETED.name())) {
                log.info(scheduler.toString());
                scheduler.dispose();
                log.warn("PARENT TASK {} COMPLETE", parentTask.getId());
                log.warn("CHILD TASK RETRIES: {}", taskChild.getRetries());
                log.warn("SCHEDULER DISPOSED: {}", scheduler.isDisposed());
            }
            return taskChild;
        } catch (DAOException ex) {
            String msg = String.format("Error updating child task %s", taskChild.toString());
            throw new ServiceException(msg, ex);
        }
    }


    private Mono<ControlMessage> deserializeControlMessage(AcknowledgableDelivery message) {
        try {
            ControlMessage controlMessage = mapper.readValue(message.getBody(), ControlMessage.class);
            return Mono.just(controlMessage);
        } catch (IOException ex) {
            // DO NOT requeue the message if it fails here!
            message.nack(false);
            return Mono.empty();
        }
    }


    /**
     * Stream the messages coming off of the CONTROL_QUEUE
     * @return A flux of ControlMessage
     */
    public Flux<ControlMessage> streamControlMessages() {
        ConsumeOptions options = new ConsumeOptions();
        options.qos(1000);
        Flux<AcknowledgableDelivery> controlMessageStream = receiver.consumeManualAck(CONTROL_QUEUE, options);
        return controlMessageStream
            .delaySubscription(sender.declareQueue(QueueSpecification.queue(CONTROL_QUEUE)))
            .flatMap(this::deserializeControlMessage);
    }
}
