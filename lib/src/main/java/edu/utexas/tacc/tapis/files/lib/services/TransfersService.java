package edu.utexas.tacc.tapis.files.lib.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jcraft.jsch.IO;
import com.rabbitmq.client.AMQP.Queue.DeleteOk;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Delivery;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.clients.HTTPClient;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.json.TapisObjectMapper;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.TransferControlAction;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskParent;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskRequestElement;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import edu.utexas.tacc.tapis.files.lib.models.TransferURI;
import edu.utexas.tacc.tapis.files.lib.rabbit.RabbitMQConnection;
import edu.utexas.tacc.tapis.files.lib.transfers.ObservableInputStream;
import edu.utexas.tacc.tapis.files.lib.utils.Utils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.jetbrains.annotations.NotNull;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.AcknowledgableDelivery;
import reactor.rabbitmq.ConsumeOptions;
import reactor.rabbitmq.ExchangeSpecification;
import reactor.rabbitmq.OutboundMessage;
import reactor.rabbitmq.OutboundMessageResult;
import reactor.rabbitmq.QueueSpecification;
import reactor.rabbitmq.RabbitFlux;
import reactor.rabbitmq.Receiver;
import reactor.rabbitmq.ReceiverOptions;
import reactor.rabbitmq.Sender;
import reactor.rabbitmq.SenderOptions;
import reactor.util.retry.Retry;

import javax.inject.Inject;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static reactor.rabbitmq.BindingSpecification.binding;

@Service
public class TransfersService {
    private static final Logger log = LoggerFactory.getLogger(TransfersService.class);
    private final String TRANSFERS_EXCHANGE = "tapis.files";
    private String PARENT_QUEUE = "tapis.files.transfers.parent";
    private String CHILD_QUEUE = "tapis.files.transfers.child";
    private String CONTROL_EXCHANGE = "tapis.files.transfers.control";
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
        init();
    }

    private void init() {

        // Initialize the exchanges and queues
        ExchangeSpecification controlExSpec = ExchangeSpecification.exchange(CONTROL_EXCHANGE)
            .type("fanout")
            .durable(true)
            .autoDelete(false);

        ExchangeSpecification transferExSpec = ExchangeSpecification.exchange(TRANSFERS_EXCHANGE)
            .type("direct")
            .durable(true)
            .autoDelete(false);

        QueueSpecification childSpec = QueueSpecification.queue(CHILD_QUEUE)
            .durable(true)
            .autoDelete(false);

        QueueSpecification parentSpec = QueueSpecification.queue(PARENT_QUEUE)
            .durable(true)
            .autoDelete(false);

        sender.declare(controlExSpec)
            .then(sender.declare(transferExSpec))
            .then(sender.declare(childSpec))
            .then(sender.declare(parentSpec))
            .then(sender.bind(binding(TRANSFERS_EXCHANGE, PARENT_QUEUE, PARENT_QUEUE)))
            .then(sender.bind(binding(TRANSFERS_EXCHANGE, CHILD_QUEUE, CHILD_QUEUE)))
            .subscribe();

    }

    public void setParentQueue(String name) {
        this.PARENT_QUEUE = name;
        init();
    }

    public void setChildQueue(String name) {
        this.CHILD_QUEUE = name;
        init();
    }

    public void setControlExchange(String name) {
        this.CONTROL_EXCHANGE = name;
        init();
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
            for (TransferTaskParent parent : parents) {
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


    /**
     * Creates the top level TransferTask and the associated TransferTaskParents that were requested in the elements List.
     *
     * @param username Obo username
     * @param tenantId tenantId
     * @param tag      Optional identifier
     * @param elements List of requested paths to transfer
     * @return TransferTask The TransferTask that has been saved to the DB
     * @throws ServiceException Saving to DB fails
     */
    public TransferTask createTransfer(@NotNull String username, @NotNull String tenantId, String tag, List<TransferTaskRequestElement> elements) throws ServiceException {

        TransferTask task = new TransferTask();
        task.setTenantId(tenantId);
        task.setUsername(username);
        task.setStatus(TransferTaskStatus.ACCEPTED);
        task.setTag(tag);

        try {
            TransferTask newTask = dao.createTransferTask(task, elements);

            for (TransferTaskParent parent : newTask.getParentTasks()) {
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
            dao.cancelTransfer(task);

            TransferControlAction action = new TransferControlAction();
            action.setAction(TransferControlAction.ControlAction.CANCEL);
            action.setCreated(Instant.now());
            action.setTenantId(task.getTenantId());
            action.setTaskId(task.getId());
            this.publishControlMessage(action);
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
            OutboundMessage message = new OutboundMessage(TRANSFERS_EXCHANGE, PARENT_QUEUE, m.getBytes());
            sender.sendWithPublishConfirms(Mono.just(message)).subscribe();
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new ServiceException(Utils.getMsg("FILES_TXFR_SVC_ERR1", task.getTenantId(), task.getUsername(),
                "publishParentTaskMessage", task.getId(), task.getUuid(), e.getMessage()), e);
        }
    }

    public void publishControlMessage(@NotNull TransferControlAction action) throws ServiceException {
        try {
            String m = mapper.writeValueAsString(action);
            OutboundMessage message = new OutboundMessage(CONTROL_EXCHANGE, "#", m.getBytes());
            Flux<OutboundMessageResult> confirms = sender.sendWithPublishConfirms(Mono.just(message));
            confirms.subscribe();
        } catch ( JsonProcessingException e) {
            log.info(e.getMessage());
            throw new ServiceException(Utils.getMsg("FILES_TXFR_SVC_ERR_PUBLISH_MESSAGE"));
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
        return parentStream;
    }

    public Flux<TransferTaskParent> processParentTasks(Flux<AcknowledgableDelivery> messageStream) {
        return messageStream
            .groupBy(m -> {
                try {
                    return groupByTenant(m);
                } catch (ServiceException ex) {
                    return Mono.empty();
                }
            })
            .flatMap(group -> {
                Scheduler scheduler = Schedulers.newBoundedElastic(10, 10, "ParentPool:" + group.key());
                return group
                    .flatMap(m ->
                        deserializeParentMessage(m)
                            .flatMap(t1 -> Mono.fromCallable(() -> doParentChevronOne(t1))
                                .publishOn(scheduler)
                                .retryWhen(
                                    Retry.backoff(MAX_RETRIES, Duration.ofSeconds(1))
                                        .maxBackoff(Duration.ofMinutes(60))
                                        .filter(e -> e.getClass() == IOException.class)
                                )
                                .onErrorResume(e -> doErrorParentChevronOne(m, e, t1))
                            )
                            .flatMap(t2 -> {
                                m.ack();
                                return Mono.just(t2);
                            })
                    );
            });
    }


    /**
     * This method handles exceptions/errors if the parent task failed.
     *
     * @param m      message from rabbitmq
     * @param e      Throwable
     * @param parent TransferTaskParent
     * @return Mono TransferTaskParent
     */
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
     * This method checks the permissions on both the source and destination of the transfer.
     *
     * @param parentTask the TransferTaskParent
     * @return boolean is/is not permitted.
     * @throws ServiceException When api calls for permissions fail
     */
    private boolean checkPermissionsForParent(TransferTaskParent parentTask) throws ServiceException {
        // For http inputs no need to do any permission checking on the source
        boolean isHttpSource = parentTask.getSourceURI().getProtocol().equalsIgnoreCase("http");
        String tenantId = parentTask.getTenantId();
        String username = parentTask.getUsername();

        String srcSystemId = parentTask.getSourceURI().getSystemId();
        String srcPath = parentTask.getSourceURI().getPath();
        String destSystemId = parentTask.getDestinationURI().getSystemId();
        String destPath = parentTask.getDestinationURI().getPath();

        // If we have a tapis:// link, have to do the source perms check
        if (!isHttpSource) {
            boolean sourcePerms = permsService.isPermitted(tenantId, username, srcSystemId, srcPath, FileInfo.Permission.READ);
            if (!sourcePerms) {
                return false;
            }
        }
        boolean destPerms = permsService.isPermitted(tenantId, username, destSystemId, destPath, FileInfo.Permission.MODIFY);
        if (!destPerms) {
            return false;
        }
        return true;
    }


    /**
     * We prepare a "bill of materials" for the total transfer task. This includes doing a recursive listing and
     * inserting the records into the DB, then publishing all of the messages to rabbitmq. After that, the child task workers
     * will pick them up and begin the actual transferring of bytes.
     *
     * @param parentTask TransferTaskParent
     * @return Updated task
     * @throws ServiceException When a listing or DAO error occurs
     */
    private TransferTaskParent doParentChevronOne(TransferTaskParent parentTask) throws ServiceException, ForbiddenException {
        log.debug("***** DOING doParentChevronOne ****");
        log.debug(parentTask.toString());
        TapisSystem sourceSystem;
        IRemoteDataClient sourceClient;

        boolean isPermitted = checkPermissionsForParent(parentTask);
        if (!isPermitted) {
            throw new ForbiddenException();
        }

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
            parentTask = dao.updateTransferTaskParent(parentTask);

        } catch (DAOException ex) {
            throw new ServiceException(Utils.getMsg("FILES_TXFR_SVC_ERR1", parentTask.getTenantId(), parentTask.getUsername(),
                "doParentChevronOneA", parentTask.getId(), parentTask.getUuid(), ex.getMessage()), ex);
        }

        try {
            TransferURI sourceURI = parentTask.getSourceURI();

            if (sourceURI.toString().startsWith("tapis://")) {
                sourceSystem = systemsCache.getSystem(parentTask.getTenantId(), sourceURI.getSystemId(), parentTask.getUsername());
                if (sourceSystem.getEnabled() == null || !sourceSystem.getEnabled()) {
                    String msg = Utils.getMsg("FILES_TXFR_SYS_NOTENABLED", parentTask.getTenantId(),
                        parentTask.getUsername(), parentTask.getId(), parentTask.getUuid(), sourceSystem.getId());
                    throw new ServiceException(msg);
                }
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
     * Helper function to publish many child task messages at once.
     *
     * @param children A list of TransferTaskChild
     */
    public void publishBulkChildMessages(List<TransferTaskChild> children) {
        Flux<OutboundMessageResult> messages = sender.sendWithPublishConfirms(
            Flux.fromIterable(children)
                .flatMap(task -> {
                    try {
                        String m = mapper.writeValueAsString(task);
                        return Flux.just(new OutboundMessage(TRANSFERS_EXCHANGE, CHILD_QUEUE, m.getBytes(StandardCharsets.UTF_8)));
                    } catch (JsonProcessingException e) {
                        return Flux.empty();
                    }
                })
        );
        messages.subscribe();
    }

    public void publishChildMessage(TransferTaskChild childTask) throws ServiceException {
        try {
            String m = mapper.writeValueAsString(childTask);
            OutboundMessage message = new OutboundMessage(TRANSFERS_EXCHANGE, CHILD_QUEUE, m.getBytes(StandardCharsets.UTF_8));

            sender.send(Mono.just(message)).subscribe();
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


    /**
     * Helper method to extract the top level taskId which we group on for all the children tasks.
     *
     * @param message Message from rabbitmq
     * @return Id of the top level task.
     */
    private int childTaskGrouper(AcknowledgableDelivery message) throws IOException {
        int taskId = mapper.readValue(message.getBody(), TransferTaskChild.class).getTaskId();
        return taskId % 10;
    }


    /**
     * This method listens on the CHILD_QUEUE and creates a Flux<AcknowledgableDelivery> that
     * we can push through the transfers workflow
     *
     * @return A flux of messages
     */
    public Flux<AcknowledgableDelivery> streamChildMessages() {
        ConsumeOptions options = new ConsumeOptions();
        options.qos(1000);
        Flux<AcknowledgableDelivery> childMessageStream = receiver.consumeManualAck(CHILD_QUEUE, options);
        return childMessageStream;
    }


    /**
     * This is the main processing workflow. Starting with the raw message stream created by streamChildMessages(),
     * we walk through the transfer process. A Flux<TransferTaskChild> is returned and can be subscribed to later
     * for further processing / notifications / logging
     *
     * @param messageStream stream of messages from RabbitMQ
     * @return a Flux of TransferTaskChild
     */
    public Flux<TransferTaskChild> processChildTasks(@NotNull Flux<AcknowledgableDelivery> messageStream) {
        // Deserialize the message so that we can pass that into the reactor chain.

        return messageStream
            .groupBy((m)-> {
                try {
                    return this.childTaskGrouper(m);
                } catch (IOException ex) {
                    return Mono.empty();
                }
            })
            .flatMap(group -> {
                Scheduler scheduler = Schedulers.newBoundedElastic(5, 100, "ChildPool:" + group.key());
                return group
                    .flatMap(
                    //We need the message in scope so we can ack/nack it later
                    m -> deserializeChildMessage(m)
                        .publishOn(scheduler)
                        .flatMap(t1 -> Mono.fromCallable(() -> chevronOne(t1))
                            .retryWhen(
                                Retry.backoff(MAX_RETRIES, Duration.ofMillis(10))
                                    .maxBackoff(Duration.ofSeconds(10))
                                    .scheduler(scheduler)
                            ).onErrorResume(e -> doErrorChevronOne(m, e, t1))
                        )
                        .flatMap(t2 -> Mono.fromCallable(() -> testRunner(t2))
                            .retryWhen(
                                Retry.backoff(MAX_RETRIES * 10, Duration.ofMillis(1000))
                                    .maxBackoff(Duration.ofMinutes(10))
                                    .scheduler(scheduler)
                                    .doBeforeRetry(signal-> log.error("RETRY", signal.failure()))
                                    .filter(e -> e.getClass().equals(IOException.class))
                            )
                            .onErrorResume(e -> doErrorChevronOne(m, e, t2))
                        )
                        .flatMap(t3 -> Mono.fromCallable(() -> chevronThree(t3))
                            .retryWhen(
                                Retry.backoff(MAX_RETRIES, Duration.ofMillis(10))
                                    .maxBackoff(Duration.ofSeconds(10))
                                    .scheduler(scheduler)
                            ).onErrorResume(e -> doErrorChevronOne(m, e, t3))
                        )
                        .flatMap(t4 -> Mono.fromCallable(() -> chevronFour(t4))
                            .retryWhen(
                                Retry.backoff(MAX_RETRIES, Duration.ofMillis(10))
                                    .maxBackoff(Duration.ofSeconds(10))
                                    .scheduler(scheduler)
                            ).onErrorResume(e -> doErrorChevronOne(m, e, t4))
                        )
                        .flatMap(t5 -> {
                            m.ack();
                            return Mono.just(t5);
                        })
                        .flatMap(t6 -> Mono.fromCallable(() -> chevronFive(t6)))
                );
            });
    }


    /**
     * Error handler for any of the steps.
     *
     * @param message Message from rabbitmq
     * @param cause   Exception that was thrown
     * @param child   the Child task that failed
     * @return Mono with the updated TransferTaskChild
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
            parent.setEndTime(Instant.now());
            parent.setErrorMessage(cause.getMessage());
            dao.updateTransferTaskParent(parent);

            //Finally the top level task
            TransferTask topTask = dao.getTransferTaskByID(child.getTaskId());
            topTask.setStatus(TransferTaskStatus.FAILED);
            topTask.setErrorMessage(cause.getMessage());
            dao.updateTransferTask(topTask);

        } catch (DAOException ex) {
            log.error(Utils.getMsg("FILES_TXFR_SVC_ERR1", child.getTenantId(), child.getUsername(),
                "doErrorChevronOne", child.getId(), child.getUuid(), ex.getMessage()), ex);
        }
        return Mono.empty();
    }

    /**
     * Step one: We update the task's status and the parent task if necessary
     *
     * @param taskChild The child transfer task
     * @return Updated TransferTaskChild
     */
    private TransferTaskChild chevronOne(@NotNull TransferTaskChild taskChild) throws ServiceException {
        log.debug("***** DOING chevronOne ****");
        try {
            TransferTaskParent parentTask = dao.getTransferTaskParentById(taskChild.getParentTaskId());
            if (parentTask.getStatus().equals(TransferTaskStatus.CANCELLED)) {
                return taskChild;
            }
            taskChild = dao.getChildTaskByUUID(taskChild.getUuid());
            taskChild.setStatus(TransferTaskStatus.IN_PROGRESS);
            taskChild.setStartTime(Instant.now());
            dao.updateTransferTaskChild(taskChild);

            // If the parent task has not been set to IN_PROGRESS do it here.
            if (!parentTask.getStatus().equals(TransferTaskStatus.IN_PROGRESS)) {
                parentTask.setStatus(TransferTaskStatus.IN_PROGRESS);
                parentTask.setStartTime(Instant.now());
                dao.updateTransferTaskParent(parentTask);
            }

            return taskChild;
        } catch (DAOException ex) {
            throw new ServiceException(Utils.getMsg("FILES_TXFR_SVC_ERR1", taskChild.getTenantId(), taskChild.getUsername(),
                "chevronOne", taskChild.getId(), taskChild.getUuid(), ex.getMessage()), ex);
        }

    }

    private TransferTaskChild cancelTransferChild(TransferTaskChild taskChild) {
        try {
            taskChild.setStatus(TransferTaskStatus.CANCELLED);
            taskChild.setEndTime(Instant.now());
            return dao.updateTransferTaskChild(taskChild);
        } catch (DAOException ex) {
            log.error("CANCEL", ex);
            return null;
        }
    }

    private TransferTaskChild testRunner(TransferTaskChild taskChild) throws ServiceException, IOException {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<TransferTaskChild> future = executorService.submit(new Callable<TransferTaskChild>() {
            @Override
            public TransferTaskChild call() throws IOException, ServiceException {
                return chevronTwo(taskChild);
            }
        });
        streamControlMessages()
            .subscribe( (message)-> {
                future.cancel(true);
            });

        try {
            // Blocking call, but the subscription above will still listen
            return future.get();
        } catch (ExecutionException ex) {
            if (ex.getCause() instanceof IOException) {
                throw new IOException(ex.getCause().getMessage(), ex.getCause());
            } else if (ex.getCause() instanceof ServiceException){
                throw new ServiceException(ex.getCause().getMessage(), ex.getCause());
            } else {
                throw new RuntimeException("TODO", ex);
            }
        } catch (CancellationException ex) {
            return cancelTransferChild(taskChild);
        } catch (Exception ex) {
          return null;
        } finally {
            executorService.shutdown();
        }
    }

    /**
     * The meat of the operation.
     *
     * @param taskChild the incoming child task
     * @return update child task
     * @throws ServiceException If the DAO updates failed or a transfer failed in flight
     */
    private TransferTaskChild chevronTwo(TransferTaskChild taskChild) throws ServiceException, NotFoundException, IOException {

        if (taskChild.isTerminal()) return taskChild;

        TapisSystem sourceSystem;
        TapisSystem destSystem;
        IRemoteDataClient sourceClient;
        IRemoteDataClient destClient;
        log.info("***** DOING chevronTwo ****");

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
        } else {
            sourcePath = sourceURL.getPath();
            sourceSystem = systemsCache.getSystem(taskChild.getTenantId(), sourceURL.getSystemId(), taskChild.getUsername());
            if (sourceSystem.getEnabled() == null || !sourceSystem.getEnabled()) {
                String msg = Utils.getMsg("FILES_TXFR_SYS_NOTENABLED", taskChild.getTenantId(),
                    taskChild.getUsername(), taskChild.getId(), taskChild.getUuid(), sourceSystem.getId());
                throw new ServiceException(msg);
            }
            sourceClient = remoteDataClientFactory.getRemoteDataClient(taskChild.getTenantId(), taskChild.getUsername(),
                sourceSystem, taskChild.getUsername());

        }

        //Step 2: Get clients for source / dest
        try {
            destSystem = systemsCache.getSystem(taskChild.getTenantId(), destURL.getSystemId(), taskChild.getUsername());
            if (destSystem.getEnabled() == null || !destSystem.getEnabled()) {
                String msg = Utils.getMsg("FILES_TXFR_SYS_NOTENABLED", taskChild.getTenantId(),
                    taskChild.getUsername(), taskChild.getId(), taskChild.getUuid(), destSystem.getId());
                throw new ServiceException(msg);
            }
            destClient = remoteDataClientFactory.getRemoteDataClient(taskChild.getTenantId(), taskChild.getUsername(),
                destSystem, taskChild.getUsername());
        } catch (IOException | ServiceException ex) {
            throw new ServiceException(Utils.getMsg("FILES_TXFR_SVC_ERR1", taskChild.getTenantId(), taskChild.getUsername(),
                "chevronTwoB", taskChild.getId(), taskChild.getUuid(), ex.getMessage()), ex);
        }

        //Step 3: Stream the file contents to dest. While the InputStream is open,
        // we put a tap on it and send events that get grouped into 1 second intervals. Progress
        // on the child tasks are updated during the reading of the source input stream.
        try (InputStream sourceStream = sourceClient.getStream(sourcePath);
             ObservableInputStream observableInputStream = new ObservableInputStream(sourceStream)
        ) {
            // Observe the progress event stream, just get the last event from
            // the past 1 second.
            final TransferTaskChild finalTaskChild = taskChild;
            observableInputStream.getEventStream()
                .window(Duration.ofMillis(1000))
                .flatMap(window -> window.takeLast(1))
                .flatMap((prog) -> this.updateProgress(prog, finalTaskChild))
                .subscribe();
            destClient.insert(destURL.getPath(), observableInputStream);

        }
        return taskChild;
    }


    /**
     * @param bytesSent total bytes sent in latest update
     * @param taskChild The transfer task that is being worked on currently
     * @return Mono with number of bytes sent
     */
    private Mono<Long> updateProgress(Long bytesSent, TransferTaskChild taskChild) {
        taskChild.setBytesTransferred(bytesSent);
        try {
            dao.updateTransferTaskChild(taskChild);
            return Mono.just(bytesSent);
        } catch (DAOException ex) {
            log.error(Utils.getMsg("FILES_TXFR_SVC_ERR1", taskChild.getTenantId(), taskChild.getUsername(),
                "updateProgress", taskChild.getId(), taskChild.getUuid(), ex.getMessage()));
            return Mono.empty();
        }
    }

    /**
     * Book keeping and cleanup. Mark the child as COMPLETE.
     *
     * @param taskChild The child transfer task
     * @return updated TransferTaskChild
     */
    private TransferTaskChild chevronThree(@NotNull TransferTaskChild taskChild) throws ServiceException {
        // If it cancelled/failed somehow, just push it through unchanged.
        if (taskChild.isTerminal()) return taskChild;
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

    /**
     * In this step, we check to see if there are unfinished children for both thw
     * top level TransferTask and the TransferTaskParent. If there all children
     * that belong to a parent are COMPLETED, then we can mark the parent as COMPLETED. Similarly
     * for the top TransferTask, if ALL children have completed, the entire transfer is done.
     *
     * @param taskChild TransferTaskChild instance
     * @return updated child
     * @throws ServiceException If the updates to the records in the DB failed
     */
    private TransferTaskChild chevronFour(@NotNull TransferTaskChild taskChild) throws ServiceException {
        if (taskChild.isTerminal()) return taskChild;
        try {
            TransferTask task = dao.getTransferTaskByID(taskChild.getTaskId());
            TransferTaskParent parent = dao.getTransferTaskParentById(taskChild.getParentTaskId());
            // Check to see if all the children are complete. If so, update the parent task
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
                }
            }

            return taskChild;
        } catch (DAOException ex) {
            throw new ServiceException(Utils.getMsg("FILES_TXFR_SVC_ERR1", taskChild.getTenantId(), taskChild.getUsername(),
                "chevronFour", taskChild.getId(), taskChild.getUuid(), ex.getMessage()), ex);
        }
    }

    /**
     * @param taskChild child task
     * @return Updated child task
     * @throws ServiceException if we can't update the record in the DB
     */
    private TransferTaskChild chevronFive(@NotNull TransferTaskChild taskChild) {
        log.info("***** DOING chevronFive **** {}", taskChild);

         return taskChild;
    }

    private Mono<TransferControlAction> deserializeControlMessage(Delivery message) {
        try {
            TransferControlAction controlMessage = mapper.readValue(message.getBody(), TransferControlAction.class);
            return Mono.just(controlMessage);
        } catch (IOException ex) {
            return Mono.empty();
        }
    }

    /**
     * Stream the messages coming off of the CONTROL_QUEUE
     *
     * @return A flux of ControlMessage
     */
    public Flux<TransferControlAction> streamControlMessages() {
        ConsumeOptions options = new ConsumeOptions();
        options.qos(1000);

        // Each new transfer needs its own queue. Autodelete them when
        // the connection closes.
        String queueName = "control." + UUID.randomUUID();
        QueueSpecification qspec = QueueSpecification.queue(queueName)
            .autoDelete(true);

        Flux<Delivery> controlMessageStream = receiver.consumeAutoAck(queueName, options);
        return controlMessageStream
            .delaySubscription(sender.declareQueue(qspec)
                .then(sender.bind(binding(CONTROL_EXCHANGE, "#", queueName)))
            ).flatMap(this::deserializeControlMessage);
    }
}
