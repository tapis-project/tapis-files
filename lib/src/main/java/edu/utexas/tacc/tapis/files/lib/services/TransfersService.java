package edu.utexas.tacc.tapis.files.lib.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP.Queue.DeleteOk;
import com.rabbitmq.client.ConnectionFactory;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.json.TapisObjectMapper;
import edu.utexas.tacc.tapis.files.lib.models.*;
import edu.utexas.tacc.tapis.files.lib.rabbit.RabbitMQConnection;
import edu.utexas.tacc.tapis.files.lib.transfers.ObservableInputStream;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.uri.TapisUrl;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.*;
import reactor.util.retry.Retry;

import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import javax.ws.rs.NotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
public class TransfersService {
    private static final Logger log = LoggerFactory.getLogger(TransfersService.class);
    private String PARENT_QUEUE = "tapis.files.transfers.parent";
    private String CHILD_QUEUE = "tapis.files.transfers.child";
    private String CONTROL_QUEUE = "tapis.files.transfers.control";
    private static final int MAX_RETRIES = 5;
    private final Receiver receiver;
    private final Sender sender;

    private final FileTransfersDAO dao;
    private final RemoteDataClientFactory remoteDataClientFactory;
    private final SystemsCache systemsCache;

    private static final TransferTaskStatus[] FINAL_STATES = new TransferTaskStatus[]{
        TransferTaskStatus.FAILED,
        TransferTaskStatus.CANCELLED,
        TransferTaskStatus.COMPLETED};
    private static final ObjectMapper mapper = TapisObjectMapper.getMapper();

    @Inject
    public TransfersService(FileTransfersDAO dao,
                            RemoteDataClientFactory remoteDataClientFactory,
                            SystemsCache systemsCache) {
        ConnectionFactory connectionFactory = RabbitMQConnection.getInstance();
        ReceiverOptions receiverOptions = new ReceiverOptions()
            .connectionFactory(connectionFactory)
            .connectionSubscriptionScheduler(Schedulers.newBoundedElastic(8, 1000, "receiver"));
        SenderOptions senderOptions = new SenderOptions()
            .connectionFactory(connectionFactory)
            .connectionSubscriptionScheduler(Schedulers.newBoundedElastic(8, 1000, "sender"));
        receiver = RabbitFlux.createReceiver(receiverOptions);
        sender = RabbitFlux.createSender(senderOptions);
        this.dao = dao;
        this.remoteDataClientFactory = remoteDataClientFactory;
        this.systemsCache = systemsCache;
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

    public List<TransferTask> getRecentTransfers(String tenantId, String username) throws ServiceException {
        try {
            return dao.getRecentTransfersForUser(tenantId, username);
        } catch (DAOException ex) {
            throw new ServiceException(ex.getMessage(), ex);
        }
    }

    public TransferTask getTransferTaskByUUID(@NotNull UUID taskUUID) throws ServiceException, NotFoundException {
        try {
            TransferTask task = dao.getTransferTaskByUUID(taskUUID);
            if (task == null) {
                throw new NotFoundException("No transfer task with this ID found.");
            }
            List<TransferTaskParent> parents = dao.getAllParentsForTaskByID(task.getId());
            task.setParentTasks(parents);

            return task;
        } catch (DAOException ex) {
            throw new ServiceException(ex.getMessage(), ex);
        }
    }

    public TransferTaskParent getTransferTaskParentByUUID(@NotNull UUID taskUUID) throws ServiceException, NotFoundException {
        try {
            TransferTaskParent task = dao.getTransferTaskParentByUUID(taskUUID);
            if (task == null) {
                throw new NotFoundException("No transfer task with this UUID found.");
            }
            return task;
        } catch (DAOException ex) {
            throw new ServiceException(ex.getMessage(), ex);
        }
    }

    public TransferTask createTransfer(@NotNull String username, @NotNull String tenantId, String tag, List<TransferTaskRequestElement> elements) throws ServiceException {

        TransferTask task = new TransferTask();
        task.setTenantId(tenantId);
        task.setUsername(username);
        task.setStatus(TransferTaskStatus.ACCEPTED.name());
        task.setTag(tag);
        try {
            // TODO: check if requesting user has access to both the source and dest systems, throw an error back if not
            // TODO: Do this in a single transaction
            TransferTask newTask = dao.createTransferTask(task, elements);
            for (TransferTaskParent parent: newTask.getParentTasks()) {
                this.publishParentTaskMessage(parent);
            }
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
            // todo: publish cancel message on the control queue
        } catch (DAOException ex) {
            throw new ServiceException(ex.getMessage(), ex);
        }
    }

    public Mono<DeleteOk> deleteQueue(String qName) {
        return sender.delete(QueueSpecification.queue(qName));
    }

    private void publishParentTaskMessage(@NotNull TransferTaskParent task) throws ServiceException {
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

    private Mono<TransferTaskParent> deserializeParentMessage(AcknowledgableDelivery message) {
        try {
            TransferTaskParent parent = mapper.readValue(message.getBody(), TransferTaskParent.class);
            return Mono.just(parent);
        } catch (IOException ex) {
            // DO NOT requeue the message if it fails here!
            message.nack(false);
            return Mono.empty();
        }
    }

    private String groupByTenant(AcknowledgableDelivery message) throws ServiceException {
        try {
            return mapper.readValue(message.getBody(), TransferTaskParent.class).getTenantId();
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

    public Flux<TransferTaskParent> processParentTasks(Flux<AcknowledgableDelivery> messageStream) {
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
                    .parallel()
                    .runOn(Schedulers.newBoundedElastic(10, 10, "ParentPool:" + group.key()))
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

    private Mono<TransferTaskParent> doErrorParentChevronOne(AcknowledgableDelivery m, Throwable e, TransferTaskParent t) {
        //TODO: Add task failure, set status etc
        t.setStatus(TransferTaskStatus.FAILED.name());
        try {
            dao.updateTransferTaskParent(t);
        } catch (DAOException ex) {
            log.error("Could not update task", ex);
        }
        log.error("Error callback", e);
        m.nack(false);
        return Mono.empty();
    }


    /**
     * We prepare a "bill of materials" for the total transfer task. This includes doing a recursive listing and
     * inserting the records into the DB, then publishing all of the messages to rabbitmq. After that, the child task workers
     * will pick them up and begin the actual transferring of bytes.
     * @param parentTask
     * @return
     * @throws ServiceException
     */
    private TransferTaskParent doParentChevronOne(TransferTaskParent parentTask) throws ServiceException {
        log.info("***** DOING doParentChevronOne ****");
        log.info(parentTask.toString());
        TSystem sourceSystem;
        IRemoteDataClient sourceClient;

        // Update the top level task first, if it is not already updateted with the startTime
        try {
            TransferTask task = dao.getTransferTaskByID(parentTask.getTaskId());
            task.setStartTime(Instant.now());
            dao.updateTransferTask(task);
        } catch (DAOException ex) {
            throw new ServiceException("Could not update task!", ex);
        }


        try {
            String sourceURI = parentTask.getSourceURI();

            if (sourceURI.startsWith("tapis://")) {
                TapisUrl sourceURL = TapisUrl.makeTapisUrl(parentTask.getSourceURI());
                sourceSystem = systemsCache.getSystem(parentTask.getTenantId(), sourceURL.getSystemId(), parentTask.getUsername());
                sourceClient = remoteDataClientFactory.getRemoteDataClient(sourceSystem, parentTask.getUsername());

                //TODO: Retries will break this, should delete anything in the DB if it is a retry?
                List<FileInfo> fileListing;
                fileListing = sourceClient.ls(sourceURL.getPath());
                List<TransferTaskChild> children = new ArrayList<>();
                long totalBytes = 0;
                for (FileInfo f : fileListing) {
                    totalBytes += f.getSize();
                    TransferTaskChild child = new TransferTaskChild(parentTask, f);
                    children.add(child);
                }
                parentTask.setTotalBytes(totalBytes);
                parentTask.setStartTime(Instant.now());
                parentTask.setStatus(TransferTaskStatus.STAGED.name());
                parentTask = dao.updateTransferTaskParent(parentTask);
                dao.bulkInsertChildTasks(children);
                children = dao.getAllChildren(parentTask);
                publishBulkChildMessages(children);
            } else if (sourceURI.startsWith("http://") || sourceURI.startsWith("https://")) {
                TransferTaskChild task = new TransferTaskChild();
                task.setSourceURI(parentTask.getSourceURI());
                task.setParentTaskId(parentTask.getId());
                task.setTaskId(parentTask.getTaskId());
                task.setDestinationURI(parentTask.getDestinationURI());
                task.setStatus(TransferTaskStatus.ACCEPTED.name());
                task.setTenantId(parentTask.getTenantId());
                task.setUsername(parentTask.getUsername());
                dao.insertChildTask(task);
            }
            return parentTask;
        } catch (DAOException | TapisException | IOException e) {
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

    private Integer groupByParentTask(AcknowledgableDelivery message) throws ServiceException {
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
                Scheduler scheduler = Schedulers.newBoundedElastic(5,100,"ChildPool:"+group.key());

                return group
                    .parallel()
                    .runOn(scheduler)
                    .flatMap(
                        //Wwe need the message in scope so we can ack/nack it later
                        m -> deserializeChildMessage(m)
                            .flatMap(t1 -> Mono.fromCallable(() -> chevronOne(t1))
                                .retryWhen(Retry.backoff(MAX_RETRIES, Duration.ofSeconds(1))
                                    .scheduler(scheduler)
                                    .maxBackoff(Duration.ofMinutes(30)))
                                .onErrorResume(e -> doErrorChevronOne(m, e, t1))
                            )
                            .flatMap(t2 -> Mono.fromCallable(() -> chevronTwo(t2))
                                .retryWhen(Retry.backoff(MAX_RETRIES, Duration.ofSeconds(1))
                                    .scheduler(scheduler)
                                    .maxBackoff(Duration.ofMinutes(30)))
                                .onErrorResume(e -> doErrorChevronOne(m, e, t2))
                            )
                            .flatMap(t3 -> Mono.fromCallable(() -> chevronThree(t3))
                                .retryWhen(Retry.backoff(MAX_RETRIES, Duration.ofSeconds(1))
                                    .scheduler(scheduler)
                                    .maxBackoff(Duration.ofMinutes(30)))
                                .onErrorResume(e -> doErrorChevronOne(m, e, t3))
                            )
                            .flatMap(t4 -> Mono.fromCallable(() -> chevronFour(t4))
                                .retryWhen(Retry.backoff(MAX_RETRIES, Duration.ofSeconds(1))
                                    .scheduler(scheduler)
                                    .maxBackoff(Duration.ofMinutes(30)))
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
            TransferTaskParent parentTask = dao.getTransferTaskParentById(taskChild.getParentTaskId());
            if (parentTask.getStatus().equals(TransferTaskStatus.CANCELLED.name())) {
                return taskChild;
            }
            taskChild = dao.getChildTaskByUUID(taskChild);
            taskChild.setStatus(TransferTaskStatus.IN_PROGRESS.name());
            taskChild.setStartTime(Instant.now());
            dao.updateTransferTaskChild(taskChild);

            // If the parent task has not been set to IN_PROGRESS do it here.
            if (!parentTask.getStatus().equals(TransferTaskStatus.IN_PROGRESS.name())) {
                parentTask.setStatus(TransferTaskStatus.IN_PROGRESS.name());
                dao.updateTransferTaskParent(parentTask);
            }
            String noteMessage = String.format("Transfer in progress for %s", taskChild.getSourceURI());
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

        TapisUrl sourceURL;
        TapisUrl destURL;
        try {
            sourceURL = TapisUrl.makeTapisUrl(taskChild.getSourceURI());
            destURL = TapisUrl.makeTapisUrl(taskChild.getDestinationURI());
        } catch (TapisException ex) {
            throw new ServiceException("Invalid URI", ex);
        }

        //Step 2: Get clients for source / dest
        try {
            sourceSystem = systemsCache.getSystem(taskChild.getTenantId(), sourceURL.getSystemId(), taskChild.getUsername() );
            sourceClient = remoteDataClientFactory.getRemoteDataClient(sourceSystem, taskChild.getUsername());
            destSystem = systemsCache.getSystem(taskChild.getTenantId(), destURL.getSystemId(), taskChild.getUsername());
            destClient = remoteDataClientFactory.getRemoteDataClient(destSystem, taskChild.getUsername());
        } catch (IOException | ServiceException ex) {
            log.error(ex.getMessage(), ex);
            throw new ServiceException(ex.getMessage(), ex);
        }

        //Step 3: Stream the file contents to dest
        try (InputStream sourceStream =  sourceClient.getStream(sourceURL.getPath());
             ObservableInputStream observableInputStream = new ObservableInputStream(sourceStream)
        ){

            // Observe the progress event stream, just get the last event from
            // the past 1 second.
            final TransferTaskChild finalTaskChild = taskChild;
            observableInputStream.getEventStream()
                .window(Duration.ofMillis(1000))
                .flatMap(window->window.takeLast(1))
                .publishOn(Schedulers.boundedElastic())
                .flatMap((prog)->this.updateProgress(prog, finalTaskChild))
                .subscribe();
            destClient.insert(destURL.getPath(), observableInputStream);

        } catch (IOException ex) {
            String msg = String.format("Error transferring %s", taskChild.toString());
            throw new ServiceException(msg, ex);
        } catch (NotFoundException ex) {
            String msg = String.format("ERROR! could not find file: %s", taskChild.getSourceURI());
            throw new ServiceException(msg, ex);
        }
        return taskChild;
    }

    private Mono<Long> updateProgress(Long aLong, TransferTaskChild taskChild) {
        taskChild.setBytesTransferred(aLong);
        try {
            dao.updateTransferTaskChild(taskChild);
            return Mono.just(aLong);
        } catch (DAOException ex) {
            String message = "Error updating progress for TransferTaskChild {}";
            log.error(message, taskChild);
            return Mono.empty();
        }
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
            taskChild.setStatus(TransferTaskStatus.COMPLETED.name());
            taskChild.setEndTime(Instant.now());
            taskChild = dao.updateTransferTaskChild(taskChild);
//            parentTask = dao.updateTransferTaskBytesTransferred(parentTask.getId(), taskChild.getBytesTransferred());
            return taskChild;
        } catch (DAOException ex) {
            String msg = String.format("Error updating child task %s", taskChild.toString());
            throw new ServiceException(msg, ex);
        }
    }

    private TransferTaskChild chevronFour(@NotNull TransferTaskChild taskChild) throws ServiceException {
        log.info("***** DOING chevronFour **** {}", taskChild);
        try {
            TransferTask task = dao.getTransferTaskByID(taskChild.getTaskId());
            TransferTaskParent parent = dao.getTransferTaskParentById(taskChild.getParentTaskId());
            // Check to see if all the children are complete. If so, update the parent task
            // TODO: Race condition
            if (!task.getStatus().equals(TransferTaskStatus.COMPLETED.name())) {
                long incompleteCount = dao.getIncompleteChildrenCount(taskChild.getTaskId());
                if (incompleteCount == 0) {
                    task.setStatus(TransferTaskStatus.COMPLETED.name());
                    task.setEndTime(Instant.now());
                    dao.updateTransferTask(task);
                }
            }

            if (!parent.getStatus().equals(TransferTaskStatus.COMPLETED.name())) {
                long incompleteCount = dao.getIncompleteChildrenCountForParent(taskChild.getParentTaskId());
                if (incompleteCount == 0) {
                    parent.setStatus(TransferTaskStatus.COMPLETED.name());
                    parent.setEndTime(Instant.now());
                    parent.setBytesTransferred(parent.getBytesTransferred() + taskChild.getBytesTransferred());
                    dao.updateTransferTaskParent(parent);
                    log.info("Updated parent task {}", parent);
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
            TransferTask task = dao.getTransferTaskByID(taskChild.getTaskId());
            if (task.getStatus().equals(TransferTaskStatus.COMPLETED.name())) {
                dao.updateTransferTask(task);
                log.info(scheduler.toString());
                log.info("PARENT TASK {} COMPLETE", taskChild);
                log.info("CHILD TASK RETRIES: {}", taskChild.getRetries());
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
