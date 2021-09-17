package edu.utexas.tacc.tapis.files.lib.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP.Queue.DeleteOk;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Delivery;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.json.TapisObjectMapper;
import edu.utexas.tacc.tapis.files.lib.models.TransferControlAction;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskParent;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskRequestElement;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import edu.utexas.tacc.tapis.files.lib.rabbit.RabbitMQConnection;
import edu.utexas.tacc.tapis.files.lib.utils.Utils;
import org.jetbrains.annotations.NotNull;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
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

import javax.inject.Inject;
import javax.ws.rs.NotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

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
            .retry(5)
            .block(Duration.ofSeconds(5));

    }

    public void setParentQueue(String name) {
        this.PARENT_QUEUE = name;
        QueueSpecification parentSpec = QueueSpecification.queue(PARENT_QUEUE)
            .durable(true)
            .autoDelete(false);
        sender.declare(parentSpec)
            .then(sender.bind(binding(TRANSFERS_EXCHANGE, PARENT_QUEUE, PARENT_QUEUE)))
            .block(Duration.ofSeconds(5));
    }

    public void setChildQueue(String name) {
        this.CHILD_QUEUE = name;
        QueueSpecification parentSpec = QueueSpecification.queue(CHILD_QUEUE)
            .durable(true)
            .autoDelete(false);
        sender.declare(parentSpec)
            .then(sender.bind(binding(TRANSFERS_EXCHANGE, CHILD_QUEUE, CHILD_QUEUE)))
            .block(Duration.ofSeconds(5));
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

    public TransferTaskChild getChildTaskByUUID(UUID uuid) throws ServiceException {
        try {
            return dao.getChildTaskByUUID(uuid);
        } catch (DAOException e) {
            throw new ServiceException(Utils.getMsg("FILES_TXFR_SVC_ERR1", null, null,
                "getChildTaskByUUID", null, uuid, e.getMessage()), e);
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

    public Flux<AcknowledgableDelivery> streamParentMessages() {
        ConsumeOptions options = new ConsumeOptions();
        options.qos(1000);
        Flux<AcknowledgableDelivery> parentStream = receiver.consumeManualAck(PARENT_QUEUE, options);
        return parentStream;
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
