package edu.utexas.tacc.tapis.files.lib.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP.Queue.DeleteOk;
import com.rabbitmq.client.ConnectionFactory;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.clients.HTTPClient;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.json.TapisObjectMapper;
import edu.utexas.tacc.tapis.files.lib.models.*;
import edu.utexas.tacc.tapis.files.lib.rabbit.RabbitMQConnection;
import edu.utexas.tacc.tapis.files.lib.transfers.ObservableInputStream;
import edu.utexas.tacc.tapis.files.lib.utils.Utils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
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
import org.jetbrains.annotations.NotNull;

import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static reactor.retry.Retry.any;

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
    private final FilePermsService permsService;

    private static final TransferTaskStatus[] FINAL_STATES = new TransferTaskStatus[]{
        TransferTaskStatus.FAILED,
        TransferTaskStatus.CANCELLED,
        TransferTaskStatus.COMPLETED};
    private static final ObjectMapper mapper = TapisObjectMapper.getMapper();

    @Inject
    public TransfersService(FileTransfersDAO dao,
                            FilePermsService permsService,
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
        this.permsService = permsService;
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

    public boolean isPermitted(@NotNull String username, @NotNull String tenantId, @NotNull UUID transferTaskUuid) throws ServiceException {
        try {
            TransferTask task = dao.getTransferTaskByUUID(transferTaskUuid);
            return task.getTenantId().equals(tenantId) && task.getUsername().equals(username);
        } catch (DAOException ex) {
            throw new ServiceException(Utils.getMsg("FILES_TXFR_SVC_ERR2", tenantId, username,
                  "isPermitted", transferTaskUuid, ex.getMessage()), ex);
        }
    }

    public TransferTask getTransferTaskDetails(UUID uuid) throws ServiceException {

        try {
            TransferTask task = dao.getTransferTaskByUUID(uuid);
            List<TransferTaskParent> parents = dao.getAllParentsForTaskByID(task.getId());
            task.setParentTasks(parents);
            for (TransferTaskParent parent: parents) {
                List<TransferTaskChild> children = dao.getAllChildren(parent);
                parent.setChildren(children);
            }
            return task;
        } catch (DAOException ex) {
            throw new ServiceException(Utils.getMsg("FILES_TXFR_SVC_ERR3",
                    "getTransferTaskDetails", uuid, ex.getMessage()), ex);
        }


    }

    public List<TransferTaskChild> getAllChildrenTasks(TransferTask task) throws ServiceException {
        try {
            return dao.getAllChildren(task);
        } catch (DAOException e) {
            throw new ServiceException(Utils.getMsg("FILES_TXFR_SVC_ERR1", task.getTenantId(), task.getUsername(),
                  "getAllChildrenTasks", task.getId(), task.getUuid(), e.getMessage()), e);
        }
    }

    public List<TransferTask> getRecentTransfers(String tenantId, String username, int limit, int offset) throws ServiceException {
        limit = Math.min(limit, 1000);
        offset = Math.max(0, offset);
        try {
            return dao.getRecentTransfersForUser(tenantId, username, limit, offset);
        } catch (DAOException ex) {
            throw new ServiceException(Utils.getMsg("FILES_TXFR_SVC_ERR5", tenantId, username,
                  "getRecentTransfers", ex.getMessage()), ex);
        }
    }

    public TransferTask getTransferTaskByUUID(@NotNull UUID taskUUID) throws ServiceException, NotFoundException {
        try {
            TransferTask task = dao.getTransferTaskByUUID(taskUUID);
            if (task == null) {
              throw new NotFoundException(Utils.getMsg("FILES_TXFR_SVC_NOT_FOUND", "getTransferTaskByUUID", taskUUID));
            }
            List<TransferTaskParent> parents = dao.getAllParentsForTaskByID(task.getId());
            task.setParentTasks(parents);

            return task;
        } catch (DAOException ex) {
            throw new ServiceException(Utils.getMsg("FILES_TXFR_SVC_ERR3",
                  "getTransferTaskByUUID", taskUUID, ex.getMessage()), ex);
        }
    }

    public TransferTask getTransferTaskById(@NotNull int id) throws ServiceException, NotFoundException {
        try {
            TransferTask task = dao.getTransferTaskByID(id);
            if (task == null) {
              throw new NotFoundException(Utils.getMsg("FILES_TXFR_SVC_NOT_FOUND", "getTransferTaskById", id));
            }
            List<TransferTaskParent> parents = dao.getAllParentsForTaskByID(task.getId());
            task.setParentTasks(parents);

            return task;
        } catch (DAOException ex) {
            throw new ServiceException(Utils.getMsg("FILES_TXFR_SVC_ERR4",
                  "getTransferTaskById", id, ex.getMessage()), ex);
        }
    }

    public TransferTaskParent getTransferTaskParentByUUID(@NotNull UUID taskUUID) throws ServiceException, NotFoundException {
        try {
            TransferTaskParent task = dao.getTransferTaskParentByUUID(taskUUID);
            if (task == null) {
              throw new NotFoundException(Utils.getMsg("FILES_TXFR_SVC_NOT_FOUND", "getTransferTaskParentByUUID", taskUUID));
            }
            return task;
        } catch (DAOException ex) {
            throw new ServiceException(Utils.getMsg("FILES_TXFR_SVC_ERR3", "getTransferTaskParentByUUID", taskUUID,
                    ex.getMessage()), ex);
        }
    }

    public TransferTask createTransfer(@NotNull String username, @NotNull String tenantId, String tag, List<TransferTaskRequestElement> elements) throws ServiceException, ForbiddenException {

        // Check ofr authorization on both source and dest systems/paths

        for (TransferTaskRequestElement elem: elements) {
            log.info("Checking permissions for transfer");
            // For http inputs no need to do any permission checking on the source
            boolean isHttpSource = elem.getSourceURI().getProtocol().equalsIgnoreCase("http");

            String srcSystemId = elem.getSourceURI().getSystemId();
            String srcPath = elem.getSourceURI().getPath();
            String destSystemId = elem.getDestinationURI().getSystemId();
            String destPath = elem.getDestinationURI().getPath();

            // If we have a tapis:// link, have to do the source perms check
            if (!isHttpSource) {
                boolean sourcePerms = permsService.isPermitted(tenantId, username, srcSystemId, srcPath, FileInfo.Permission.READ);
                if (!sourcePerms) {
                    String msg = Utils.getMsg("FILES_NOT_AUTHORIZED", tenantId, username, srcSystemId, srcPath);
                    throw new NotAuthorizedException(msg);
                }
            }
            boolean destPerms = permsService.isPermitted(tenantId, username, destSystemId, destPath, FileInfo.Permission.READ);
            if (!destPerms) {
                String msg = Utils.getMsg("FILES_NOT_AUTHORIZED", tenantId, username, destSystemId, destPath);
                throw new NotAuthorizedException(msg);
            }
            log.info("Permissions checks complete for");
            log.info(elem.toString());
        }

        TransferTask task = new TransferTask();
        task.setTenantId(tenantId);
        task.setUsername(username);
        task.setStatus(TransferTaskStatus.ACCEPTED);
        task.setTag(tag);
        try {
            TransferTask newTask = dao.createTransferTask(task, elements);
            for (TransferTaskParent parent: newTask.getParentTasks()) {
                this.publishParentTaskMessage(parent);
            }
            return newTask;
        } catch (DAOException ex) {
            throw new ServiceException(Utils.getMsg("FILES_TXFR_SVC_ERR6", tenantId, username, "createTransfer", tag,
                  ex.getMessage()), ex);
        }
    }

    public TransferTaskChild createTransferTaskChild(@NotNull TransferTaskChild task) throws ServiceException {
        try {
            return dao.insertChildTask(task);
        } catch (DAOException ex) {
            throw new ServiceException(Utils.getMsg("FILES_TXFR_SVC_ERR1", task.getTenantId(), task.getUsername(),
                  "createTransferTaskChild", task.getId(), task.getUuid(), ex.getMessage()), ex);
        }
    }

    public void cancelTransfer(@NotNull TransferTask task) throws ServiceException, NotFoundException {
        try {
            task.setStatus(TransferTaskStatus.CANCELLED);
            dao.updateTransferTask(task);
            // todo: publish cancel message on the control queue
        } catch (DAOException ex) {
            throw new ServiceException(Utils.getMsg("FILES_TXFR_SVC_ERR1", task.getTenantId(), task.getUsername(),
                  "cancelTransfer", task.getId(), task.getUuid(), ex.getMessage()), ex);
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
            throw new ServiceException(Utils.getMsg("FILES_TXFR_SVC_ERR1", task.getTenantId(), task.getUsername(),
                    "publishParentTaskMessage", task.getId(), task.getUuid(), e.getMessage()), e);
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
            message.nack(false);
            String msg = Utils.getMsg("FILES_TXFR_SVC_ERR11", ex.getMessage());
            log.error(msg);
            throw new ServiceException(msg, ex);
        }
    }


    public Flux<AcknowledgableDelivery> streamParentMessages() {
        ConsumeOptions options = new ConsumeOptions();
        options.qos(1000);
        Flux<AcknowledgableDelivery> parentStream = receiver.consumeManualAck(PARENT_QUEUE, options);
        return parentStream
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
                        .flatMap(t1->Mono.fromCallable(()-> doParentChevronOne(t1))
                                .retryWhen(
                                    Retry.backoff(MAX_RETRIES, Duration.ofSeconds(1))
                                            .maxBackoff(Duration.ofMinutes(60))
                                            .filter(e -> e.getClass() == IOException.class)
                                )
                            .onErrorResume(e -> doErrorParentChevronOne(m, e, t1))
                        )
                        .flatMap(t2->{
                            m.ack();
                            return Mono.just(t2);
                        })
                    )
            );
    }

    private Mono<TransferTaskParent> doErrorParentChevronOne(AcknowledgableDelivery m, Throwable e, TransferTaskParent parent) {
        log.error(Utils.getMsg("FILES_TXFR_SVC_ERR7", parent.toString()));
        log.error(Utils.getMsg("FILES_TXFR_SVC_ERR7", e));
        m.nack(false);

        //TODO: UPDATE this when the Optional stuff gets integrated
        try {
            TransferTask task = dao.getTransferTaskByID(parent.getTaskId());
            if (task == null) {
                return Mono.empty();
            }
            task.setStatus(TransferTaskStatus.FAILED);
            task.setEndTime(Instant.now());
            task.setErrorMessage(e.getMessage());
            dao.updateTransferTask(task);
        } catch (DAOException ex) {
            log.error(Utils.getMsg("FILES_TXFR_SVC_ERR8", parent.getTaskId(), parent.getUuid()));
        }

        parent.setStatus(TransferTaskStatus.FAILED);
        parent.setEndTime(Instant.now());
        parent.setErrorMessage(e.getMessage());
        try {
            parent = dao.updateTransferTaskParent(parent);
            // This should really never happen, it means that the parent with that ID
            // was not even in the database.
            if (parent == null) {
                return Mono.empty();
            }
            return Mono.just(parent);
        } catch (DAOException ex) {
            log.error(Utils.getMsg("FILES_TXFR_SVC_ERR9", parent.getTaskId(), parent.getUuid()));
        }
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
        log.debug("***** DOING doParentChevronOne ****");
        log.debug(parentTask.toString());
        TapisSystem sourceSystem;
        IRemoteDataClient sourceClient;

        // Update the top level task first, if it is not already updated with the startTime
        try {
            TransferTask task = dao.getTransferTaskByID(parentTask.getTaskId());
            if (task.getStartTime() == null) {
                task.setStartTime(Instant.now());
                task.setStatus(TransferTaskStatus.IN_PROGRESS);
                dao.updateTransferTask(task);
            }

            //update parent task status, start time
            parentTask.setStatus(TransferTaskStatus.IN_PROGRESS);
            parentTask.setStartTime(Instant.now());

        } catch (DAOException ex) {
            throw new ServiceException(Utils.getMsg("FILES_TXFR_SVC_ERR1", parentTask.getTenantId(), parentTask.getUsername(),
                  "doParentChevronOneA", parentTask.getId(), parentTask.getUuid(), ex.getMessage()), ex);
        }

        try {
            TransferURI sourceURI = parentTask.getSourceURI();

            if (sourceURI.toString().startsWith("tapis://")) {
                sourceSystem = systemsCache.getSystem(parentTask.getTenantId(), sourceURI.getSystemId(), parentTask.getUsername());
                sourceClient = remoteDataClientFactory.getRemoteDataClient(parentTask.getTenantId(), parentTask.getUsername(),
                                                                           sourceSystem, parentTask.getUsername());

                //TODO: Retries will break this, should delete anything in the DB if it is a retry?
                List<FileInfo> fileListing;
                fileListing = sourceClient.ls(sourceURI.getPath());
                List<TransferTaskChild> children = new ArrayList<>();
                long totalBytes = 0;
                for (FileInfo f : fileListing) {
                    totalBytes += f.getSize();
                    TransferTaskChild child = new TransferTaskChild(parentTask, f);
                    children.add(child);
                }
                parentTask.setTotalBytes(totalBytes);
                parentTask.setStatus(TransferTaskStatus.STAGED);
                parentTask = dao.updateTransferTaskParent(parentTask);
                dao.bulkInsertChildTasks(children);
                children = dao.getAllChildren(parentTask);
                publishBulkChildMessages(children);
            } else if (sourceURI.toString().startsWith("http://") || sourceURI.toString().startsWith("https://")) {
                TransferTaskChild task = new TransferTaskChild();
                task.setSourceURI(parentTask.getSourceURI());
                task.setParentTaskId(parentTask.getId());
                task.setTaskId(parentTask.getTaskId());
                task.setDestinationURI(parentTask.getDestinationURI());
                task.setStatus(TransferTaskStatus.ACCEPTED);
                task.setTenantId(parentTask.getTenantId());
                task.setUsername(parentTask.getUsername());
                task = dao.insertChildTask(task);
                publishChildMessage(task);
                parentTask.setStatus(TransferTaskStatus.STAGED);
                parentTask = dao.updateTransferTaskParent(parentTask);
            }
            return parentTask;
        } catch (DAOException | TapisException | IOException e) {
            throw new ServiceException(Utils.getMsg("FILES_TXFR_SVC_ERR1", parentTask.getTenantId(), parentTask.getUsername(),
                  "doParentChevronOneB", parentTask.getId(), parentTask.getUuid(), e.getMessage()), e);
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
            throw new ServiceException(Utils.getMsg("FILES_TXFR_SVC_ERR1", childTask.getTenantId(), childTask.getUsername(),
                  "publishChildMessage", childTask.getId(), childTask.getUuid(), ex.getMessage()), ex);
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
            String msg = Utils.getMsg("FILES_TXFR_SVC_ERR12", ex.getMessage());
            log.error(msg);
            throw new ServiceException(msg, ex);
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
     * @param messageStream stream of messages from RabbitMQ
     * @return a Flux of TransferTaskChild
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
                                .retryWhen(
                                    Retry.backoff(MAX_RETRIES, Duration.ofSeconds(0))
                                        .maxBackoff(Duration.ofSeconds(10))
                                        .scheduler(scheduler)
                                ).onErrorResume(e -> doErrorChevronOne(m, e, t1))
                            )
                            .flatMap(t2 -> Mono.fromCallable(() -> chevronTwo(t2))
                                .retryWhen(
                                    Retry.backoff(MAX_RETRIES, Duration.ofMillis(100))
                                        .maxBackoff(Duration.ofMinutes(30))
                                        .scheduler(scheduler)
                                        .filter(e -> e.getClass() == IOException.class)
//                                        .filter(e -> e.getClass() != TapisException.class)
                                ).onErrorResume(e -> doErrorChevronOne(m, e, t2))
                            )
                            .flatMap(t3 -> Mono.fromCallable(() -> chevronThree(t3))
                                .retryWhen(
                                    Retry.backoff(MAX_RETRIES, Duration.ofSeconds(0))
                                        .maxBackoff(Duration.ofSeconds(10))
                                        .scheduler(scheduler)
                                ).onErrorResume(e -> doErrorChevronOne(m, e, t3))
                            )
                            .flatMap(t4 -> Mono.fromCallable(() -> chevronFour(t4))
                                .retryWhen(
                                    Retry.backoff(MAX_RETRIES, Duration.ofSeconds(0))
                                        .maxBackoff(Duration.ofSeconds(10))
                                        .scheduler(scheduler)
                                ).onErrorResume(e -> doErrorChevronOne(m, e, t4))
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
     * @param cause Exception that was thrown
     * @param child the Child task that failed
     * @return
     */
    private Mono<TransferTaskChild> doErrorChevronOne(AcknowledgableDelivery message, Throwable cause, TransferTaskChild child) {
        message.nack(false);
        log.error(Utils.getMsg("FILES_TXFR_SVC_ERR10", child.toString()));

        //TODO: Fix this for the "optional" flag
        try {

            // Child First
            child.setStatus(TransferTaskStatus.FAILED);
            child.setErrorMessage(cause.getMessage());
            dao.updateTransferTaskChild(child);

            //Now parent
            TransferTaskParent parent = dao.getTransferTaskParentById(child.getParentTaskId());
            parent.setStatus(TransferTaskStatus.FAILED);
            parent.setErrorMessage(cause.getMessage());
            dao.updateTransferTaskParent(parent);

            //Finally the top level task
            TransferTask topTask = dao.getTransferTaskByID(child.getTaskId());
            topTask.setStatus(TransferTaskStatus.FAILED);
            topTask.setErrorMessage(cause.getMessage());
            dao.updateTransferTask(topTask);

        } catch (DAOException ignored) {
            log.error(Utils.getMsg("FILES_TXFR_SVC_ERR1", child.getTenantId(), child.getUsername(),
                  "doErrorChevronOne", child.getId(), child.getUuid(), ignored.getMessage()), ignored);
        }
        return Mono.empty();
    }

    /**
     * Step one: We update the task's status and the parent task if necessary
     *
     * @param taskChild
     * @return
     */
    private TransferTaskChild chevronOne(@NotNull TransferTaskChild taskChild) throws ServiceException {
        log.debug("***** DOING chevronOne ****");
        try {
            TransferTaskParent parentTask = dao.getTransferTaskParentById(taskChild.getParentTaskId());
            if (parentTask.getStatus().equals(TransferTaskStatus.CANCELLED)) {
                return taskChild;
            }
            taskChild = dao.getChildTaskByUUID(taskChild);
            taskChild.setStatus(TransferTaskStatus.IN_PROGRESS);
            taskChild.setStartTime(Instant.now());
            dao.updateTransferTaskChild(taskChild);

            // If the parent task has not been set to IN_PROGRESS do it here.
            if (!parentTask.getStatus().equals(TransferTaskStatus.IN_PROGRESS)) {
                parentTask.setStatus(TransferTaskStatus.IN_PROGRESS);
                dao.updateTransferTaskParent(parentTask);
            }
            return taskChild;
        } catch (DAOException ex) {
            throw new ServiceException(Utils.getMsg("FILES_TXFR_SVC_ERR1", taskChild.getTenantId(), taskChild.getUsername(),
                  "chevronOne", taskChild.getId(), taskChild.getUuid(), ex.getMessage()), ex);
        }

    }


    /**
     * Chevron 2: The meat of the operation. We get remote clients for source and dest and send bytes
     * @param taskChild
     * @return
     * @throws ServiceException
     */
    private TransferTaskChild chevronTwo(TransferTaskChild taskChild) throws ServiceException, NotFoundException, IOException, TapisException {
        log.debug("***** DOING chevronTwo ****");
        log.debug(taskChild.toString());
        TapisSystem sourceSystem;
        TapisSystem destSystem;
        IRemoteDataClient sourceClient;
        IRemoteDataClient destClient;

        //Step 1: Update task in DB to IN_PROGRESS and increment the retries on this particular task
        try {
            taskChild.setStatus(TransferTaskStatus.IN_PROGRESS);
            taskChild.setRetries(taskChild.getRetries() + 1);
            taskChild = dao.updateTransferTaskChild(taskChild);
        } catch (DAOException ex) {
            throw new ServiceException(Utils.getMsg("FILES_TXFR_SVC_ERR1", taskChild.getTenantId(), taskChild.getUsername(),
                  "chevronTwoA", taskChild.getId(), taskChild.getUuid(), ex.getMessage()), ex);
        }

        String sourcePath;
        TransferURI destURL;
        TransferURI sourceURL = taskChild.getSourceURI();

        destURL = taskChild.getDestinationURI();

        if (taskChild.getSourceURI().toString().startsWith("https://") || taskChild.getSourceURI().toString().startsWith("http://")) {
            sourceClient = new HTTPClient(taskChild.getTenantId(), taskChild.getUsername(), sourceURL.toString(), destURL.toString());
            //This should be the full string URL such as http://google.com
            sourcePath = sourceURL.toString();
        } else  {
            sourcePath = sourceURL.getPath();
            sourceSystem = systemsCache.getSystem(taskChild.getTenantId(), sourceURL.getSystemId(), taskChild.getUsername());
            sourceClient = remoteDataClientFactory.getRemoteDataClient(taskChild.getTenantId(), taskChild.getUsername(),
                                                                       sourceSystem, taskChild.getUsername());

        }

        //Step 2: Get clients for source / dest
        try {
            destSystem = systemsCache.getSystem(taskChild.getTenantId(), destURL.getSystemId(), taskChild.getUsername());
            destClient = remoteDataClientFactory.getRemoteDataClient(taskChild.getTenantId(), taskChild.getUsername(),
                                                                     destSystem, taskChild.getUsername());
        } catch (IOException | ServiceException ex) {
            throw new ServiceException(Utils.getMsg("FILES_TXFR_SVC_ERR1", taskChild.getTenantId(), taskChild.getUsername(),
                  "chevronTwoB", taskChild.getId(), taskChild.getUuid(), ex.getMessage()), ex);
        }

        //Step 3: Stream the file contents to dest
        try (InputStream sourceStream =  sourceClient.getStream(sourcePath);
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

        }
        return taskChild;
    }

    private Mono<Long> updateProgress(Long aLong, TransferTaskChild taskChild) {
        taskChild.setBytesTransferred(aLong);
        try {
            dao.updateTransferTaskChild(taskChild);
            return Mono.just(aLong);
        } catch (DAOException ex) {
            log.error(Utils.getMsg("FILES_TXFR_SVC_ERR1", taskChild.getTenantId(), taskChild.getUsername(),
                    "updateProgress", taskChild.getId(), taskChild.getUuid(), ex.getMessage()));
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
            taskChild.setStatus(TransferTaskStatus.COMPLETED);
            taskChild.setEndTime(Instant.now());
            taskChild = dao.updateTransferTaskChild(taskChild);
            return taskChild;
        } catch (DAOException ex) {
            throw new ServiceException(Utils.getMsg("FILES_TXFR_SVC_ERR1", taskChild.getTenantId(), taskChild.getUsername(),
                  "chevronThree", taskChild.getId(), taskChild.getUuid(), ex.getMessage()), ex);
        }
    }

    private TransferTaskChild chevronFour(@NotNull TransferTaskChild taskChild) throws ServiceException {
        log.info("***** DOING chevronFour **** {}", taskChild);
        try {
            TransferTask task = dao.getTransferTaskByID(taskChild.getTaskId());
            TransferTaskParent parent = dao.getTransferTaskParentById(taskChild.getParentTaskId());
            // Check to see if all the children are complete. If so, update the parent task
            // TODO: Race condition?
            if (!task.getStatus().equals(TransferTaskStatus.COMPLETED)) {
                long incompleteCount = dao.getIncompleteChildrenCount(taskChild.getTaskId());
                if (incompleteCount == 0) {
                    task.setStatus(TransferTaskStatus.COMPLETED);
                    task.setEndTime(Instant.now());
                    dao.updateTransferTask(task);
                }
            }

            if (!parent.getStatus().equals(TransferTaskStatus.COMPLETED)) {
                long incompleteCount = dao.getIncompleteChildrenCountForParent(taskChild.getParentTaskId());
                if (incompleteCount == 0) {
                    parent.setStatus(TransferTaskStatus.COMPLETED);
                    parent.setEndTime(Instant.now());
                    parent.setBytesTransferred(parent.getBytesTransferred() + taskChild.getBytesTransferred());
                    dao.updateTransferTaskParent(parent);
                    log.info("Updated parent task {}", parent);
                }
            }

            return taskChild;
        } catch (DAOException ex) {
            throw new ServiceException(Utils.getMsg("FILES_TXFR_SVC_ERR1", taskChild.getTenantId(), taskChild.getUsername(),
                  "chevronFour", taskChild.getId(), taskChild.getUuid(), ex.getMessage()), ex);
        }
    }

    private TransferTaskChild chevronFive(@NotNull TransferTaskChild taskChild, Scheduler scheduler) throws ServiceException {
        log.info("***** DOING chevronFive **** {}", taskChild);
        try {
            TransferTask task = dao.getTransferTaskByID(taskChild.getTaskId());
            if (task.getStatus().equals(TransferTaskStatus.COMPLETED)) {
                dao.updateTransferTask(task);
                log.info(scheduler.toString());
                log.info("PARENT TASK {} COMPLETE", taskChild);
                log.info("CHILD TASK RETRIES: {}", taskChild.getRetries());
            }
            return taskChild;
        } catch (DAOException ex) {
            throw new ServiceException(Utils.getMsg("FILES_TXFR_SVC_ERR1", taskChild.getTenantId(), taskChild.getUsername(),
                  "chevronFive", taskChild.getId(), taskChild.getUuid(), ex.getMessage()), ex);
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
