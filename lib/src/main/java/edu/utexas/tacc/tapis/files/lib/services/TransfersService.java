package edu.utexas.tacc.tapis.files.lib.services;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import javax.inject.Inject;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotFoundException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.shared.utils.PathUtils;
import edu.utexas.tacc.tapis.files.lib.models.TransferURI;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCacheNoAuth;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.json.TapisObjectMapper;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo.Permission;
import edu.utexas.tacc.tapis.files.lib.models.TransferControlAction;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskParent;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskRequestElement;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import edu.utexas.tacc.tapis.files.lib.rabbit.RabbitMQConnection;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import static edu.utexas.tacc.tapis.files.lib.services.FileOpsService.SVCLIST_SHAREDCTX;

/*
 * Transfers service methods providing functionality for TransfersApiService and TestTransfers
 */
@Service
public class TransfersService
{
  private static final Logger log = LoggerFactory.getLogger(TransfersService.class);
  private static final String impersonationIdNull = null;
  private static final String TRANSFERS_EXCHANGE = "tapis.files";
  private static String PARENT_QUEUE = "tapis.files.transfers.parent";
  private static String CHILD_QUEUE = "tapis.files.transfers.child";
  private static String PARENT_EXCHANGE = "tapis.files.transfers.parent.exchange";
  private static String CHILD_EXCHANGE = "tapis.files.transfers.child.exchange";
  public static String CONTROL_EXCHANGE = "tapis.files.transfers.control";
  private static String CHILD_ROUTING_KEY = "child";
  public static String PARENT_ROUTING_KEY = "parent";
  private final FileTransfersDAO dao;
  private final FileOpsService fileOpsService;

  private Connection connection;
  private static final TransferTaskStatus[] FINAL_STATES = new TransferTaskStatus[]
  {
    TransferTaskStatus.FAILED, TransferTaskStatus.CANCELLED, TransferTaskStatus.COMPLETED
  };

  private static final ObjectMapper mapper = TapisObjectMapper.getMapper();

  // TODO/TBD Can we inject all services needed? Seems like when trying this before it did not work out, had to pass
  //          them in via the constructor? Something to do with test setup?

  // **************** Inject Services using HK2 ****************
  @Inject
  FilePermsService permsService;
  @Inject
  FileShareService shareService;
  @Inject
  RemoteDataClientFactory remoteDataClientFactory;
  @Inject
  ServiceContext serviceContext;
  @Inject
  SystemsCache systemsCache;
  @Inject
  SystemsCacheNoAuth systemsCacheNoAuth;

  /*
   * Constructor for service.
   * Note that this is never invoked explicitly. Arguments of constructor are initialized via Dependency Injection.
   */
  @Inject
  public TransfersService(FileTransfersDAO dao1, SystemsCache cache1, FileOpsService svc1, FilePermsService svc2)
  {
    dao = dao1;
    systemsCache = cache1;
    fileOpsService = svc1;
    permsService = svc2;
    init();
  }

  // ************************************************************************
  // *********************** Public Methods *********************************
  // ************************************************************************
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
        String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR1", null, null, "getChildTaskByUUID", null, null, uuid, e.getMessage());
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
                                     "getAllChildrenTasks", task.getId(), task.getTag(), task.getUuid(), e.getMessage());
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

      // Update tag for each parent task to be created.
      for (TransferTaskRequestElement e : elements) { e.setTag(tag); }

      // Check for srcSharedCtx or destSharedCtx in the request elements.
      // Only certain services may set these to true. May throw ForbiddenException.
      for (TransferTaskRequestElement e : elements) { checkSharedCtxAllowed(rUser, tag, e); }

      // Validate the request. Check that all Tapis systems exist and are enabled.
      // Check that transfer between system types is supported.
      validateRequest(rUser, tag, elements);

      // Create and init the transfer task
      TransferTask task = new TransferTask();
      task.setTenantId(rUser.getOboTenantId());
      task.setUsername(rUser.getOboUserId());
      task.setStatus(TransferTaskStatus.ACCEPTED);
      task.setTag(tag);

      // Persist the transfer task and queue up parent transfer tasks.
      try
      {
        // Persist the transfer task to the DB
        log.trace(LibUtils.getMsgAuthR("FILES_TXFR_PERSIST_TASK", rUser, tag, elements.size()));
        TransferTask newTask = dao.createTransferTask(task, elements);
        // Put the parent transfer tasks onto the queue for asynchronous processing.
        for (TransferTaskParent parent : newTask.getParentTasks())
        {
          publishParentTaskMessage(parent);
        }
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
                                     "createTransferTaskChild", task.getId(), task.getTag(), task.getUuid(), ex.getMessage());
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
                                     "cancelTransfer", task.getId(), task.getTag(), task.getUuid(), ex.getMessage());
        log.error(msg, ex);
        throw new ServiceException(msg, ex);
      }
    }

    public void publishControlMessage(@NotNull TransferControlAction action) throws ServiceException
    {
      Channel channel = null;

      try
      {
        String m = mapper.writeValueAsString(action);
        AMQP.BasicProperties properties = null;
        channel = connection.createChannel();
        channel.exchangeDeclare(TransfersService.CONTROL_EXCHANGE, BuiltinExchangeType.FANOUT, true);
        channel.basicPublish(CONTROL_EXCHANGE, "#", properties, m.getBytes());
      }
      catch (JsonProcessingException e)
      {
        log.error(e.getMessage(), e);
        throw new ServiceException(LibUtils.getMsg("FILES_TXFR_SVC_ERR_PUBLISH_MESSAGE"));
      } catch (IOException e) {
        throw new RuntimeException(e);
      } finally {
        closeChannel(channel);
      }
    }

    /**
     * Helper function to publish many child task messages at once.
     *
     * @param children A list of TransferTaskChild
     */
    public void publishBulkChildMessages(List<TransferTaskChild> children) throws ServiceException
    {
      for(TransferTaskChild child : children) {
        publishChildMessage(child);
      }
    }

    public void publishChildMessage(TransferTaskChild childTask) throws ServiceException
    {

      Channel channel = null;
      try
      {
        channel = connection.createChannel();
        String m = mapper.writeValueAsString(childTask);
        channel.basicPublish(CHILD_EXCHANGE, CHILD_ROUTING_KEY, MessageProperties.PERSISTENT_TEXT_PLAIN, m.getBytes());
      } catch (IOException e) {
        throw new RuntimeException(e.getMessage(), e);
      } finally {
        closeChannel(channel);
      }

}

  // ************************************************************************
  // *********************** Private Methods ********************************
  // ************************************************************************

  /**
   * Initialize the RabbitMQ exchanges and queues.
   */
  private void init()
  {

    try {
      connection = RabbitMQConnection.getInstance().newConnection();
      TransfersService.declareRabbitMQObjects(connection);
    } catch (Exception ex) {
      // TODO: fix this correctly
      throw new RuntimeException(ex.getMessage());
    }
  }

  public static void declareRabbitMQObjects(Connection connection) throws IOException, TimeoutException {
    Channel channel = connection.createChannel();
    channel.queueDeclare(PARENT_QUEUE, true, false, false, null);
    channel.queueDeclare(CHILD_QUEUE, true, false, false, null);
    channel.exchangeDeclare(PARENT_EXCHANGE, BuiltinExchangeType.DIRECT, true, false, null);
    channel.exchangeDeclare(CHILD_EXCHANGE, BuiltinExchangeType.DIRECT, true, false, null);
    channel.queueBind(CHILD_QUEUE, CHILD_EXCHANGE, CHILD_ROUTING_KEY);
    channel.queueBind(PARENT_QUEUE, PARENT_EXCHANGE, PARENT_ROUTING_KEY);
    channel.close();
  }

  public boolean isConnectionOk() {
    if(connection == null) {
      return false;
    }
    // Quick check to see if Rabbit is responding.
    return connection.isOpen();
  }

  /*
   * Publish a parent task to RabbitMQ PARENT_QUEUE
   */
  private void publishParentTaskMessage(@NotNull TransferTaskParent task) throws ServiceException
  {
    Channel channel = null;
    try
    {
      channel = connection.createChannel();
      String m = mapper.writeValueAsString(task);
      channel.basicPublish(PARENT_EXCHANGE, PARENT_ROUTING_KEY, MessageProperties.PERSISTENT_TEXT_PLAIN, m.getBytes());
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage(), e);
    } finally {
      closeChannel(channel);
    }
  }

  /**
   * Confirm that caller is allowed to set sharedCtx.
   * Must be a service request from a service in the allowed list.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param tag - txfr task tag
   * @param e - Transfer task element to check
   * @throws ForbiddenException - user not authorized to perform operation
   */
  private void checkSharedCtxAllowed(ResourceRequestUser rUser, String tag, TransferTaskRequestElement e)
          throws ForbiddenException
  {
    // If nothing to check or sharedCtxGrantor not set for src or dest then we are done.
    if (e == null) return;
    String srcGrantor = e.getSrcSharedCtx();
    String dstGrantor = e.getDestSharedCtx();

    // Grantor not null indicates attempt to share.
    boolean srcShared = !StringUtils.isBlank(srcGrantor);
    boolean dstShared = !StringUtils.isBlank(dstGrantor);

    // If no sharing we are done
    if (!srcShared && !dstShared) return;

    String srcSysId = e.getSourceURI().getSystemId();
    String srcPath = e.getSourceURI().getPath();
    String dstSysId = e.getDestinationURI().getSystemId();
    String dstPath = e.getDestinationURI().getPath();

    // If a service request the username will be the service name. E.g. files, jobs, streams, etc
    boolean allowed = (rUser.isServiceRequest() && SVCLIST_SHAREDCTX.contains(rUser.getJwtUserId()));
    if (allowed)
    {
      // Src and/or Dest shared, log it
      if (srcShared) log.trace(LibUtils.getMsgAuthR("FILES_AUTH_SHAREDCTX_SRC_TXFR", rUser, tag, srcSysId, srcPath, srcGrantor));
      if (dstShared) log.trace(LibUtils.getMsgAuthR("FILES_AUTH_SHAREDCTX_DST_TXFR", rUser, tag, dstSysId, dstPath, dstGrantor));
    }
    else
    {
      // Sharing not allowed. Log systems and paths involved
      String msg = LibUtils.getMsgAuthR("FILES_UNAUTH_SHAREDCTX_TXFR", rUser, tag, srcSysId, srcPath, dstSysId,
                                        srcGrantor, dstPath, dstGrantor);
      log.warn(msg);
      throw new ForbiddenException(msg);
    }
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

    // For any Tapis systems make sure they exist and are enabled (with authorization checks)
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
   * Make sure a Tapis system exists and is enabled (with authorization checking for READ)
   * For any not found or not enabled add a message to the list of error messages.
   * We check for READ access (owner, shared)
   * NOTE: Catch all exceptions, so we can collect and report as many errors as possible.
   */
  private void validateSystemIsEnabled(ResourceRequestUser rUser, String opName, String sysId, String pathStr,
                                       String sharedCtxGrantor, Permission perm, List<String> errMessages)
  {
    TapisSystem sys = null;
    // Get normalized path relative to system rootDir and protect against ../..
    String relPathStr = PathUtils.getRelativePath(pathStr).toString();
    try
    {
      // Fetch system with credentials including auth checks for system and path
      sys = LibUtils.getResolvedSysWithAuthCheck(rUser, shareService, systemsCache, systemsCacheNoAuth, permsService,
                                                 opName, sysId, relPathStr, perm, impersonationIdNull, sharedCtxGrantor);
    }
    catch (Exception e) { errMessages.add(e.getMessage()); }
  }

  /*
   * Make sure we support transfers between each pair of systems in a transfer request
   *   - If one is GLOBUS then both must be GLOBUS
   *   - dstSys must use tapis protocol (http/s not supported)
   * For any combinations not supported add a message to the list of error messages.
   * NOTE: Catch all exceptions, so we can collect and report as many errors as possible.
   *
   * @param rUser - AuthenticatedUser, contains user info needed to fetch systems
   * @param txfrElements - List of transfer elements
   * @param errMessages - List for collecting error messages
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
      String srcGrantor = txfrElement.getSrcSharedCtx();
      String dstGrantor = txfrElement.getDestSharedCtx();
      String impersonationIdNull = null;

      // Get any Tapis systems. If protocol is http/s then leave as null.
      // For protocol tapis:// get each system. These should already be in the cache due to a previous check, see validateSystemsAreEnabled()
      TapisSystem srcSys = null, dstSys = null;
      try
      {
        if (!StringUtils.isBlank(srcId) && srcUri.isTapisProtocol())
          srcSys = systemsCache.getSystem(rUser.getOboTenantId(), srcId, rUser.getOboUserId(), impersonationIdNull, srcGrantor);
        if (!StringUtils.isBlank(dstId) && dstUri.isTapisProtocol())
          dstSys = systemsCache.getSystem(rUser.getOboTenantId(), dstId, rUser.getOboUserId(), impersonationIdNull, dstGrantor);
      }
      catch (Exception e)
      {
        // In theory this will not happen due to previous check, see validateSystemsAreEnabled()
        errMessages.add(e.getMessage());
      }

      // If srcSys is GLOBUS and dstSys is not then we do not support it
      if ((srcSys != null && SystemTypeEnum.GLOBUS.equals(srcSys.getSystemType())) &&
          (dstSys == null || !SystemTypeEnum.GLOBUS.equals(dstSys.getSystemType())))
      {
        errMessages.add(LibUtils.getMsg("FILES_TXFR_GLOBUS_NOTSUPPORTED", srcUri, dstUri, txfrElement.getTag()));
      }
      // If dstSys is GLOBUS and srcSys is not then we do not support it
      if ((dstSys != null && SystemTypeEnum.GLOBUS.equals(dstSys.getSystemType())) &&
          (srcSys == null || !SystemTypeEnum.GLOBUS.equals(srcSys.getSystemType())))
      {
        errMessages.add(LibUtils.getMsg("FILES_TXFR_GLOBUS_NOTSUPPORTED", srcUri, dstUri, txfrElement.getTag()));
      }
      // If dstUri is not tapis protocol we do not support it
      if (!dstUri.isTapisProtocol())
      {
        errMessages.add(LibUtils.getMsg("FILES_TXFR_DST_NOTSUPPORTED", srcUri, dstUri, txfrElement.getTag()));
      }
    }
  }

  /*
   * For any Tapis systems make sure they exist and are enabled.
   * For any not found or not enabled add a message to the list of error messages.
   * NOTE: Catch all exceptions, so we can collect and report as many errors as possible.
   *
   * @param rUser - AuthenticatedUser, contains user info needed to fetch systems
   * @param txfrElements - List of transfer elements
   * @param errMessages - List for collecting error messages
   */
  private void validateSystemsAreEnabled(ResourceRequestUser rUser, List<TransferTaskRequestElement> txfrElements,
                                         List<String> errMessages)
  {
    // Check each Tapis system involved. Protocol is tapis:// in the URI
    for (TransferTaskRequestElement txfrElement : txfrElements)
    {
      TransferURI srcUri = txfrElement.getSourceURI();
      TransferURI dstUri = txfrElement.getDestinationURI();
      String srcSystemId = srcUri.getSystemId();
      String dstSystemId = dstUri.getSystemId();
      String srcGrantor = txfrElement.getSrcSharedCtx();
      String dstGrantor = txfrElement.getDestSharedCtx();
      // Check source system
      if (!StringUtils.isBlank(srcSystemId) && srcUri.isTapisProtocol())
      {
        validateSystemIsEnabled(rUser, "srcSystemValidate", srcSystemId, srcUri.getPath(), srcGrantor,
                                Permission.READ, errMessages);
      }
      // Check destination system
      if (!StringUtils.isBlank(dstSystemId) && dstUri.isTapisProtocol())
      {
        validateSystemIsEnabled(rUser, "dstSystemValidate", dstSystemId, dstUri.getPath(), dstGrantor,
                                Permission.MODIFY, errMessages);
      }
    }
  }



  private void closeChannel(Channel channel) throws ServiceException {
    try {
      if ((channel != null) && (channel.isOpen())) {
        channel.close();
      } else {
        log.info("Channel not open");
      }
    } catch (TimeoutException | IOException ex) {
      // TODO:  fix error message
      throw new ServiceException("Unable to close channel", ex);
    }
  }

  public void cleanup() throws IOException {
    if(isConnectionOk()) {
      connection.close();
    }
  }
}
