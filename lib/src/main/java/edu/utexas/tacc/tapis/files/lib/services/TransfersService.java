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
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.*;

import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import javax.ws.rs.NotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
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

    private Mono<String> groupByTenant(AcknowledgableDelivery message) {
        try {
            return Mono.just(mapper.readValue(message.getBody(), TransferTask.class).getTenantId());
        } catch (IOException ex) {
            log.error("invalid message", ex);
            return Mono.error(ex);
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
            .groupBy(this::groupByTenant)
            .flatMap(group ->
                group
                    .publishOn(Schedulers.newBoundedElastic(20,1,"pool"+group.key()))
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
        log.error(e.getMessage(), e);
        m.nack(false);
        return Mono.empty();
    }

    public TransferTask doParentChevronOne(TransferTask parentTask) throws ServiceException {
        log.info("***** DOING doParentChevronOne ****");
        TSystem sourceSystem;
        IRemoteDataClient sourceClient;
        // Has to be final for lambda below
        final TransferTask finalParentTask = parentTask;
        try {
            SystemsClient systemsClient = systemsClientFactory.getClient(parentTask.getTenantId(), parentTask.getUsername());
            sourceSystem = systemsClient.getSystemByName(parentTask.getSourceSystemId(), null);
            sourceClient = remoteDataClientFactory.getRemoteDataClient(sourceSystem, parentTask.getUsername());

            //TODO: Bulk inserts for both
            //TODO: Retries will break this, should delete anything in the DB if it is a retry?
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
            String noteMessage = String.format("Transfer %s staged", parentTask.getUuid());
            notificationsService.sendNotification(parentTask.getTenantId(), parentTask.getUsername(), noteMessage);
            return parentTask;
        } catch (Exception e) {
            throw new ServiceException(e.getMessage(), e);
        }
    }


    /**
     * Child task message processing
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
            return Mono.empty();
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
            .map(group -> group.publishOn(Schedulers.newBoundedElastic(6, 1 , "childPool::" + group.key())))
            .flatMap(g ->
                g.flatMap(
                    //Wwe need the message in scope so we can ack/nack it later
                    m -> deserializeChildMessage(m)
                        .takeUntilOther(streamControlMessages().filter(ctrl->ctrl.getAction().equals("CANCEL")))
                        .flatMap(t1 -> Mono.fromCallable(() -> chevronOne(t1)).retry(MAX_RETRIES).onErrorResume(e -> doErrorChevronOne(m, e, t1)))
                        .flatMap(t2 -> Mono.fromCallable(() -> chevronTwo(t2)).retry(MAX_RETRIES).onErrorResume(e -> doErrorChevronOne(m, e, t2)))
                        .flatMap(t3 -> Mono.fromCallable(() -> chevronThree(t3)).retry(MAX_RETRIES).onErrorResume(e -> doErrorChevronOne(m, e, t3)))
                        .flatMap(t4 -> Mono.fromCallable(() -> chevronFour(t4)).retry(MAX_RETRIES).onErrorResume(e -> doErrorChevronOne(m, e, t4)))
                        .flatMap(t5 -> {
                            m.ack();
                            return Mono.just(t5);
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
                .limitRate(100, 10)
                .publishOn(Schedulers.elastic())
                .subscribe(this::updateProgress);
            destClient.insert(taskChild.getDestinationPath(), observableInputStream);
        } catch (IOException | NotFoundException ex) {
            String msg = String.format("Error transferring %s", taskChild.toString());
            throw new ServiceException(msg, ex);

        }
        return taskChild;
    }

    private void updateProgress(Long aLong) {
        log.info(aLong.toString());
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

    public Flux<ControlMessage> streamControlMessages() {
        ConsumeOptions options = new ConsumeOptions();
        options.qos(1000);
        Flux<AcknowledgableDelivery> controlMessageStream = receiver.consumeManualAck(CONTROL_QUEUE, options);
        return controlMessageStream
            .delaySubscription(sender.declareQueue(QueueSpecification.queue(CONTROL_QUEUE)))
            .flatMap(this::deserializeControlMessage);
    }
}
