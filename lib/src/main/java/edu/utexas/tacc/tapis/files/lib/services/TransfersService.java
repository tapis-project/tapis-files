package edu.utexas.tacc.tapis.files.lib.services;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import javax.inject.Inject;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.Queue.DeleteOk;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Delivery;
import org.apache.commons.lang3.StringUtils;
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
import reactor.util.retry.RetrySpec;

import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.models.TransferURI;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
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
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import static reactor.rabbitmq.BindingSpecification.binding;
import static edu.utexas.tacc.tapis.files.lib.services.FileOpsService.SVCLIST_SHAREDAPPCTX;

@Service
public class TransfersService
{
  private static final Logger log = LoggerFactory.getLogger(TransfersService.class);
  private static final String TRANSFERS_EXCHANGE = "tapis.files";
  private static String PARENT_QUEUE = "tapis.files.transfers.parent";
  private static String CHILD_QUEUE = "tapis.files.transfers.child";
  private static String CONTROL_EXCHANGE = "tapis.files.transfers.control";
  private final Receiver receiver;
  private final Sender sender;

  private final FileTransfersDAO dao;
  private final SystemsCache systemsCache;

  private static final TransferTaskStatus[] FINAL_STATES = new TransferTaskStatus[]
  {
    TransferTaskStatus.FAILED, TransferTaskStatus.CANCELLED, TransferTaskStatus.COMPLETED
  };

  private static final ObjectMapper mapper = TapisObjectMapper.getMapper();

  /*
   * Constructor for service.
   * Note that this is never invoked explicitly. Arguments of constructor are initialized via Dependency Injection.
   */
  @Inject
  public TransfersService(FileTransfersDAO dao1, SystemsCache systemsCache1)
  {
    ConnectionFactory connectionFactory = RabbitMQConnection.getInstance();
    ReceiverOptions receiverOptions = new ReceiverOptions()
            .connectionMonoConfigurator( cm -> cm.retryWhen(RetrySpec.backoff(3, Duration.ofSeconds(5))) )
            .connectionFactory(connectionFactory)
            .connectionSubscriptionScheduler(Schedulers.newBoundedElastic(8, 1000, "receiver"));
    SenderOptions senderOptions = new SenderOptions()
            .connectionMonoConfigurator( cm -> cm.retryWhen(RetrySpec.backoff(3, Duration.ofSeconds(5))) )
            .connectionFactory(connectionFactory)
            .connectionSubscriptionScheduler(Schedulers.newBoundedElastic(8, 1000, "sender"));
    receiver = RabbitFlux.createReceiver(receiverOptions);
    sender = RabbitFlux.createSender(senderOptions);
    dao = dao1;
    systemsCache = systemsCache1;
    init();
  }

  // ************************************************************************
  // *********************** Public Methods *********************************
  // ************************************************************************

    public Mono<AMQP.Queue.BindOk> setParentQueue(String name)
    {
      PARENT_QUEUE = name;
      QueueSpecification parentSpec = QueueSpecification.queue(PARENT_QUEUE).durable(true).autoDelete(false);
      return sender.declare(parentSpec).then(sender.bind(binding(TRANSFERS_EXCHANGE, PARENT_QUEUE, PARENT_QUEUE)));
    }

    public Mono<AMQP.Queue.BindOk> setChildQueue(String name)
    {
      CHILD_QUEUE = name;
      QueueSpecification parentSpec = QueueSpecification.queue(CHILD_QUEUE).durable(true).autoDelete(false);
      return sender.declare(parentSpec).then(sender.bind(binding(TRANSFERS_EXCHANGE, CHILD_QUEUE, CHILD_QUEUE)));
    }

    public Mono<DeleteOk> deleteQueue(String qName)
    {
      return sender.unbind(binding(TRANSFERS_EXCHANGE, qName, qName)).then(sender.delete(QueueSpecification.queue(qName)));
    }

    public void setControlExchange(String name)
    {
      CONTROL_EXCHANGE = name;
      init();
    }

    public boolean isPermitted(@NotNull String username, @NotNull String tenantId, @NotNull UUID transferTaskUuid)
            throws ServiceException
    {
      try
      {
        TransferTask task = dao.getTransferTaskByUUID(transferTaskUuid);
        return task.getTenantId().equals(tenantId) && task.getUsername().equals(username);
      }
      catch (DAOException ex)
      {
        String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR2", tenantId, username,
                                     "isPermitted", transferTaskUuid, ex.getMessage());
        log.error(msg, ex);
        throw new ServiceException(msg, ex);
      }
    }

    public TransferTask getTransferTaskDetails(UUID uuid) throws ServiceException
    {
        try {
            TransferTask task = dao.getTransferTaskByUUID(uuid);
            List<TransferTaskParent> parents = dao.getAllParentsForTaskByID(task.getId());
            task.setParentTasks(parents);
            for (TransferTaskParent parent : parents) {
                List<TransferTaskChild> children = dao.getAllChildren(parent);
                parent.setChildren(children);
            }
            return task;
        }
        catch (DAOException ex)
        {
          String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR3", "getTransferTaskDetails", uuid, ex.getMessage());
          log.error(msg, ex);
          throw new ServiceException(msg, ex);
        }
    }

    public TransferTaskChild getChildTaskByUUID(UUID uuid) throws ServiceException
    {
      try
      {
        return dao.getChildTaskByUUID(uuid);
      }
      catch (DAOException e)
      {
        String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR1", null, null, "getChildTaskByUUID", null, uuid, e.getMessage());
        log.error(msg, e);
        throw new ServiceException(msg, e);
      }
    }

    public List<TransferTaskChild> getAllChildrenTasks(TransferTask task)
            throws ServiceException
    {
      try
      {
        return dao.getAllChildren(task);
      }
      catch (DAOException e)
      {
        String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR1", task.getTenantId(), task.getUsername(),
                                     "getAllChildrenTasks", task.getId(), task.getUuid(), e.getMessage());
        log.error(msg, e);
        throw new ServiceException(msg, e);
      }
    }

    public List<TransferTask> getRecentTransfers(String tenantId, String username, int limit, int offset)
            throws ServiceException
    {
      limit = Math.min(limit, 1000);
      offset = Math.max(0, offset);
      try
      {
        return dao.getRecentTransfersForUser(tenantId, username, limit, offset);
      }
      catch (DAOException ex)
      {
        String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR5", tenantId, username, "getRecentTransfers", ex.getMessage());
        log.error(msg, ex);
        throw new ServiceException(msg, ex);
      }
    }

    public TransferTask getTransferTaskByUUID(@NotNull UUID taskUUID) throws ServiceException, NotFoundException
    {
      try
      {
        TransferTask task = dao.getTransferTaskByUUID(taskUUID);
        if (task == null)
        {
          String msg = LibUtils.getMsg("FILES_TXFR_SVC_NOT_FOUND", "getTransferTaskByUUID", taskUUID);
          log.error(msg);
          throw new NotFoundException(msg);
        }
        List<TransferTaskParent> parents = dao.getAllParentsForTaskByID(task.getId());
        task.setParentTasks(parents);
        return task;
      }
      catch (DAOException ex)
      {
        String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR3", "getTransferTaskByUUID", taskUUID, ex.getMessage());
        log.error(msg, ex);
        throw new ServiceException(msg, ex);
      }
    }

    public TransferTask getTransferTaskById(@NotNull int id) throws ServiceException, NotFoundException
    {
      try
      {
        TransferTask task = dao.getTransferTaskByID(id);
        if (task == null)
        {
          String msg = LibUtils.getMsg("FILES_TXFR_SVC_NOT_FOUND", "getTransferTaskById", id);
          log.error(msg);
          throw new NotFoundException(msg);
        }
        List<TransferTaskParent> parents = dao.getAllParentsForTaskByID(task.getId());
        task.setParentTasks(parents);
        return task;
      }
      catch (DAOException ex)
      {
        String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR4", "getTransferTaskById", id, ex.getMessage());
        log.error(msg, ex);
        throw new ServiceException(msg, ex);
      }
    }

    public TransferTaskParent getTransferTaskParentByUUID(@NotNull UUID taskUUID)
            throws ServiceException, NotFoundException
    {
      try
      {
        TransferTaskParent task = dao.getTransferTaskParentByUUID(taskUUID);
        if (task == null)
        {
          String msg = LibUtils.getMsg("FILES_TXFR_SVC_NOT_FOUND", "getTransferTaskParentByUUID", taskUUID);
          log.error(msg);
          throw new NotFoundException(msg);
        }
        return task;
      }
      catch (DAOException ex)
      {
        String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR3", "getTransferTaskParentByUUID", taskUUID, ex.getMessage());
        log.error(msg, ex);
        throw new ServiceException(msg, ex);
      }
    }

    /**
     * Creates the top level TransferTask in table transfer_tasks
     * Creates the associated TransferTaskParents in the table transfer_tasks_parent
     *   using the provided elements list.
     *
     * @param rUser - ResourceRequestUser containing tenant, user and request info
     * @param tag      Optional identifier
     * @param elements List of requested paths to transfer
     * @return TransferTask The TransferTask that has been saved to the DB
     * @throws ServiceException Saving to DB fails
     * @throws ForbiddenException - user not authorized, only certain services authorized
     */
    public TransferTask createTransfer(@NotNull ResourceRequestUser rUser, String tag,
                                       List<TransferTaskRequestElement> elements)
            throws ServiceException
    {
      String opName = "createTransfer";
      // Make sure we have at least one element for transfer.
      if (elements == null || elements.isEmpty())
      {
        String msg = LibUtils.getMsgAuthR("FILES_TXFR_SVC_NO_ELEMENTS", rUser, "createTransfer", tag);
        throw new ServiceException(msg);
      }

      // Validate the request. Check that all Tapis systems exist and are enabled.
      // Check that transfer between system types is supported.
      validateRequest(rUser, tag, elements);

      // Create and init the transfer task
      TransferTask task = new TransferTask();
      task.setTenantId(rUser.getOboTenantId());
      task.setUsername(rUser.getOboUserId());
      task.setStatus(TransferTaskStatus.ACCEPTED);
      task.setTag(tag);

      // Check for srcSharedAppCtx or destSharedAppCtx in the request elements.
      // Only certain services may set these to true. May throw ForbiddenException.
      for (TransferTaskRequestElement e : elements) { checkSharedAppCtxAllowed(rUser, opName, tag, e); }

      // Persist the transfer task
      try
      {
        // Persist the transfer task to the DB
        log.trace(LibUtils.getMsgAuthR("FILES_TXFR_PERSIST_TASK", rUser, tag, elements.size()));
        TransferTask newTask = dao.createTransferTask(task, elements);
        // Put the transfer task onto the queue for asynchronous processing.
        for (TransferTaskParent parent : newTask.getParentTasks()) { publishParentTaskMessage(parent); }
        return newTask;
      }
      catch (DAOException | ServiceException e)
      {
        String msg = LibUtils.getMsgAuthR("FILES_TXFR_SVC_ERR6", rUser, "createTransfer", tag, e.getMessage());
        throw new ServiceException(msg, e);
      }
    }

    /*
     * Create a transfer task child in the DB. Currently only used for testing.
     * Might be able to make this package-private
     */
    public TransferTaskChild createTransferTaskChild(@NotNull TransferTaskChild task) throws ServiceException
    {
      try { return dao.insertChildTask(task); }
      catch (DAOException ex)
      {
        String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR1", task.getTenantId(), task.getUsername(),
                                     "createTransferTaskChild", task.getId(), task.getUuid(), ex.getMessage());
        log.error(msg, ex);
        throw new ServiceException(msg, ex);
      }
    }

    public void cancelTransfer(@NotNull TransferTask task) throws ServiceException, NotFoundException
    {
      try
      {
        dao.cancelTransfer(task);
        TransferControlAction action = new TransferControlAction();
        action.setAction(TransferControlAction.ControlAction.CANCEL);
        action.setCreated(Instant.now());
        action.setTenantId(task.getTenantId());
        action.setTaskId(task.getId());
        publishControlMessage(action);
      }
      catch (DAOException ex)
      {
        String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR1", task.getTenantId(), task.getUsername(),
                                     "cancelTransfer", task.getId(), task.getUuid(), ex.getMessage());
        log.error(msg, ex);
        throw new ServiceException(msg, ex);
      }
    }

    public void publishControlMessage(@NotNull TransferControlAction action) throws ServiceException
    {
      try
      {
        String m = mapper.writeValueAsString(action);
        OutboundMessage message = new OutboundMessage(CONTROL_EXCHANGE, "#", m.getBytes());
        Flux<OutboundMessageResult> confirms = sender.sendWithPublishConfirms(Mono.just(message));
        confirms.subscribe();
      }
      catch (JsonProcessingException e)
      {
        log.error(e.getMessage(), e);
        throw new ServiceException(LibUtils.getMsg("FILES_TXFR_SVC_ERR_PUBLISH_MESSAGE"));
      }
    }

    /**
     * Helper function to publish many child task messages at once.
     *
     * @param children A list of TransferTaskChild
     */
    public void publishBulkChildMessages(List<TransferTaskChild> children)
    {
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

    public void publishChildMessage(TransferTaskChild childTask) throws ServiceException
    {
      try
      {
        String m = mapper.writeValueAsString(childTask);
        OutboundMessage message = new OutboundMessage(TRANSFERS_EXCHANGE, CHILD_QUEUE, m.getBytes(StandardCharsets.UTF_8));
        sender.send(Mono.just(message)).subscribe();
      }
      catch (JsonProcessingException ex)
      {
        String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR1", childTask.getTenantId(), childTask.getUsername(),
                                     "publishChildMessage", childTask.getId(), childTask.getUuid(), ex.getMessage());
        log.error(msg, ex);
        throw new ServiceException(msg, ex);
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
        QueueSpecification childSpec = QueueSpecification.queue(CHILD_QUEUE)
            .durable(true)
            .autoDelete(false);
        return receiver.consumeManualAck(CHILD_QUEUE, options)
            .delaySubscription(sender.declareQueue(childSpec));
    }

    public Flux<AcknowledgableDelivery> streamParentMessages() {
        ConsumeOptions options = new ConsumeOptions();
        options.qos(1000);
        QueueSpecification parentSpec = QueueSpecification.queue(PARENT_QUEUE)
            .durable(true)
            .autoDelete(false);
        return receiver.consumeManualAck(PARENT_QUEUE, options)
            .delaySubscription(sender.declareQueue(parentSpec));

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

  // ************************************************************************
  // *********************** Private Methods ********************************
  // ************************************************************************

  /**
   * Initialize the RabbitMQ exchanges and queues.
   */
  private void init()
  {
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

  private void publishParentTaskMessage(@NotNull TransferTaskParent task) throws ServiceException
  {
    try
    {
      String m = mapper.writeValueAsString(task);
      OutboundMessage message = new OutboundMessage(TRANSFERS_EXCHANGE, PARENT_QUEUE, m.getBytes());
      sender.sendWithPublishConfirms(Mono.just(message)).subscribe();
      int childCount = task.getChildren() == null ? 0 : task.getChildren().size();
      log.trace(LibUtils.getMsg("FILES_TXFR_PARENT_PUBLISHED", task.getTaskId(), childCount, m));
    }
    catch (Exception e)
    {
      String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR1", task.getTenantId(), task.getUsername(),
                                   "publishParentTaskMessage", task.getId(), task.getUuid(), e.getMessage());
      throw new ServiceException(msg, e);
    }
  }

  private Mono<TransferControlAction> deserializeControlMessage(Delivery message)
  {
    try
    {
      TransferControlAction controlMessage = mapper.readValue(message.getBody(), TransferControlAction.class);
      return Mono.just(controlMessage);
    }
    catch (IOException ex) { return Mono.empty(); }
  }

  /**
   * Confirm that caller is allowed to set sharedAppCtx.
   * Must be a service request from a service in the allowed list.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param opName - operation name
   * @param tag - txfr task tag
   * @param e - Transfer task element to check
   * @throws ForbiddenException - user not authorized to perform operation
   */
  private void checkSharedAppCtxAllowed(ResourceRequestUser rUser, String opName, String tag,
                                        TransferTaskRequestElement e)
          throws ForbiddenException
  {
    // If nothing to check or sharedAppCtx not true then we are done.
    if (e == null) return;
    if (!e.isSrcSharedAppCtx() && !e.isDestSharedAppCtx()) return;

    // If a service request the username will be the service name. E.g. files, jobs, streams, etc
    String svcName = rUser.getJwtUserId();
    if (!rUser.isServiceRequest() || !SVCLIST_SHAREDAPPCTX.contains(svcName))
    {
      String msg = LibUtils.getMsgAuthR("FILES_UNAUTH_SHAREDAPPCTX_TXFR", rUser, opName, tag);
      log.warn(msg);
      throw new ForbiddenException(msg);
    }
    // An allowed service is skipping auth, log it
    log.trace(LibUtils.getMsgAuthR("FILES_AUTH_SHAREDAPPCTX_TXFR", rUser, opName, tag));
  }

  /**
   * Check that all source and destination systems referenced in a TransferRequest exist and are enabled
   * Check that based on the src and dst system types we support the transfer
   *
   * @param rUser - AuthenticatedUser, contains user info needed to fetch systems
   */
  private void validateRequest(ResourceRequestUser rUser, String tag, List<TransferTaskRequestElement> elements)
          throws ServiceException
  {
    var errMessages = new ArrayList<String>();

    // For any Tapis systems make sure they exist and are enabled.
    validateSystemsAreEnabled(rUser, elements, errMessages);

    // Check that we support transfers between each pair of systems
    validateSystemsForTxfrSupport(rUser, elements, errMessages);

    // If we have any errors log a message and return BAD_REQUEST response.
    if (!errMessages.isEmpty())
    {
      // Construct message reporting all errors
      String allErrors = LibUtils.getListOfTxfrErrors(rUser, tag, errMessages);
      log.error(allErrors);
      throw new ServiceException(allErrors);
    }
  }

  /**
   * Make sure system exists and is enabled
   * For any not found or not enabled add a message to the list of error messages.
   *
   * @param rUser - AuthenticatedUser, contains user info needed to fetch systems
   * @param sysId system to check
   * @param errMessages - List where error message are being collected
   */
  private void validateSystemForTxfr(ResourceRequestUser rUser, String sysId, List<String> errMessages)
  {
    try { LibUtils.getSystemIfEnabled(rUser, systemsCache, sysId); }
    catch (NotFoundException e)
    {
      errMessages.add(e.getMessage());
    }
  }

  /*
   * Make sure we support transfers between each pair of systems in a transfer request
   *   - If one is GLOBUS then both must be GLOBUS
   *   - dstSys must use tapis protocol (http/s not supported)
   * For any combinations not supported add a message to the list of error messages.
   *
   * @param rUser - AuthenticatedUser, contains user info needed to fetch systems
   * @param txfrElements - List of transfer elements
   * @param errMessages - List where error message are being collected
   */
  private void validateSystemsForTxfrSupport(ResourceRequestUser rUser, List<TransferTaskRequestElement> txfrElements,
                                             List<String> errMessages)
  {
    // Check each pair of systems
    for (TransferTaskRequestElement txfrElement : txfrElements)
    {
      TransferURI srcUri = txfrElement.getSourceURI();
      TransferURI dstUri = txfrElement.getDestinationURI();
      String srcId = srcUri.getSystemId();
      String dstId = dstUri.getSystemId();

      // Get any Tapis systems. If protocol is http/s then leave as null.
      // For protocol tapis:// get each system. These should already be in the cache due to a previous check, see validateSystems()
      TapisSystem srcSys = null, dstSys = null;
      try
      {
        if (!StringUtils.isBlank(srcId) && srcUri.isTapisProtocol())
          srcSys = systemsCache.getSystem(rUser.getOboTenantId(), srcId, rUser.getOboUserId());
        if (!StringUtils.isBlank(dstId) && dstUri.isTapisProtocol())
          dstSys = systemsCache.getSystem(rUser.getOboTenantId(), dstId, rUser.getOboUserId());
      }
      catch (ServiceException e)
      {
        // In theory this will not happen due to previous check, see validateSystems()
        errMessages.add(e.getMessage());
      }

      // If srcSys is GLOBUS and dstSys is not then we do not support it
      if ((srcSys != null && SystemTypeEnum.GLOBUS.equals(srcSys.getSystemType())) &&
              (dstSys == null || !SystemTypeEnum.GLOBUS.equals(dstSys.getSystemType())))
      {
        errMessages.add(LibUtils.getMsg("FILES_TXFR_GLOBUS_NOTSUPPORTED", srcUri, dstUri));
      }
      // If dstSys is GLOBUS and srcSys is not then we do not support it
      if ((dstSys != null && SystemTypeEnum.GLOBUS.equals(dstSys.getSystemType())) &&
              (srcSys == null || !SystemTypeEnum.GLOBUS.equals(srcSys.getSystemType())))
      {
        errMessages.add(LibUtils.getMsg("FILES_TXFR_GLOBUS_NOTSUPPORTED", srcUri, dstUri));
      }
      // If dstUri is not tapis protocol we do not support it
      if (!dstUri.isTapisProtocol())
      {
        errMessages.add(LibUtils.getMsg("FILES_TXFR_DST_NOTSUPPORTED", srcUri, dstUri));
      }
    }
  }

  /*
   * For any Tapis systems make sure they exist and are enabled.
   * For any not found or not enabled add a message to the list of error messages.
   *
   * @param rUser - AuthenticatedUser, contains user info needed to fetch systems
   * @param txfrElements - List of transfer elements
   * @param errMessages - List where error message are being collected
   */
  private void validateSystemsAreEnabled(ResourceRequestUser rUser, List<TransferTaskRequestElement> txfrElements,
                                         List<String> errMessages)
  {
    // Collect full set of Tapis systems involved. Protocol is tapis:// in the URI
    var allSystems = new HashSet<String>();
    for (TransferTaskRequestElement txfrElement : txfrElements)
    {
      TransferURI srcUri = txfrElement.getSourceURI();
      TransferURI dstUri = txfrElement.getDestinationURI();
      String srcId = srcUri.getSystemId();
      String dstId = dstUri.getSystemId();
      if (!StringUtils.isBlank(srcId) && srcUri.isTapisProtocol()) allSystems.add(srcId);
      if (!StringUtils.isBlank(dstId) && dstUri.isTapisProtocol()) allSystems.add(dstId);
    }

    // Make sure each system exists and is enabled
    for (String sysId : allSystems) { validateSystemForTxfr(rUser, sysId, errMessages); }
  }
}
