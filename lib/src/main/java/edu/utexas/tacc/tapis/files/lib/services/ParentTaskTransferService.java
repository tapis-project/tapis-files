package edu.utexas.tacc.tapis.files.lib.services;

import javax.inject.Inject;
import javax.ws.rs.WebApplicationException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskParent;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import edu.utexas.tacc.tapis.files.lib.models.TransferURI;
import edu.utexas.tacc.tapis.files.lib.rabbit.RabbitMQConnection;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.utils.PathUtils;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.apache.commons.lang3.StringUtils;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.json.TapisObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/*
 * Transfers service methods providing functionality for TransfersApp (a worker).
 *
 * When this class is constructed, a connection is made to RabbitMQ.  When the startListeners
 * method is called, a number of channels are created, and consumers are setup.  The end result
 * is a pool of listeners that can call handleMessage (each on it's own thread).  Threads are
 * handled by rabbitMQ via an Executor service passed in when the connection is created.
 *
 * The only thing the parent processor must do is create the child tasks and send a message for
 * each via RabbitMQ / child queue.  (for more, see the info in TransfersApp).
 */
@Service
public class ParentTaskTransferService {
  // this ends up being the maximum number of un-acked items.  This include all items in progress as well as items
  // in the queue.  So for example if there are 5 threads and this is set to 10, we will have 5 items in progress and
  // 5 items in the queue
  private static final int QOS = 2;
  private static final int MAX_CONSUMERS = RuntimeSettings.get().getParentThreadPoolSize();
  private static final int maxRetries = 3;
  private final TransfersService transfersService;
  private final FileTransfersDAO dao;
  private static final ObjectMapper mapper = TapisObjectMapper.getMapper();
  private final RemoteDataClientFactory remoteDataClientFactory;
  private final FilePermsService permsService;
  private final SystemsCache systemsCache;
  private final FileOpsService fileOpsService;
  private static final Logger log = LoggerFactory.getLogger(ParentTaskTransferService.class);
  private static String PARENT_QUEUE = "tapis.files.transfers.parent";
  private final Connection connection;
  private ExecutorService connectionThreadPool;
  private List<Channel> channels = new ArrayList<Channel>();
  private ScheduledExecutorService channelMonitorService = Executors.newSingleThreadScheduledExecutor();


  /* *********************************************************************** */
  /*            Constructors                                                 */
  /* *********************************************************************** */

  /*
   * Constructor for service.
   * Note that this is never invoked explicitly. Arguments of constructor are initialized via Dependency Injection.
   */
  @Inject
  public ParentTaskTransferService(TransfersService transfersService,
                                   FileTransfersDAO dao,
                                   FileOpsService fileOpsService,
                                   FilePermsService permsService,
                                   RemoteDataClientFactory remoteDataClientFactory,
                                   SystemsCache systemsCache) throws Exception {
    this.transfersService = transfersService;
    this.dao = dao;
    this.fileOpsService = fileOpsService;
    this.systemsCache = systemsCache;
    this.remoteDataClientFactory = remoteDataClientFactory;
    this.permsService = permsService;

    connectionThreadPool = Executors.newFixedThreadPool(MAX_CONSUMERS);
    connection = RabbitMQConnection.getInstance().newConnection(connectionThreadPool);
  }

  public void startListeners() throws IOException, TimeoutException {
    createChannels();

    channelMonitorService.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        try {
          // discard any closed channels
          Iterator<Channel> channelIterator = channels.iterator();
          while (channelIterator.hasNext()) {
            Channel channel = channelIterator.next();
            if (!channel.isOpen()) {
              log.warn("RabbitMQ channel is closed");
              channelIterator.remove();
            }
          }

          // re-open channels
          try {
            createChannels();
          } catch (Exception ex) {
            log.error("Unable to re-open channels", ex);
          }
        } catch (Throwable th) {
          String msg = LibUtils.getMsg("FILES_TXFR_CLEANUP_FAILURE");
          log.warn(msg, th);

        }
      }
    }, 5, 5, TimeUnit.MINUTES);
  }

  private void createChannels() throws IOException, TimeoutException {
    int channelsToOpen = MAX_CONSUMERS - channels.size();
    if(channelsToOpen == 0) {
      return;
    }

    log.info("Opening " + channelsToOpen + " rabbitmq channels");
    ParentTaskTransferService service = this;
    for (int i = 0; i < channelsToOpen; i++) {
      Channel channel = connection.createChannel();
      channel.basicQos(QOS);

      TransfersService.declareRabbitMQObjects(connection);

      channel.basicConsume(PARENT_QUEUE, false, new DefaultConsumer(channel) {
        @Override
        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
          try {
            service.handleDelivery(channel, consumerTag, envelope, properties, body);
          } catch (Throwable th) {
            String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR_CONSUME_MESSAGE", consumerTag);
            log.error(msg, th);;
          }
        }
      });
      channels.add(channel);
    }
  }

  public void handleDelivery(Channel channel, String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
          throws IOException{
    TransferTaskParent taskParent = null;
    try {
      String jsonMessage = new String(body, StandardCharsets.UTF_8);
      taskParent = TapisObjectMapper.getMapper().readValue(jsonMessage, TransferTaskParent.class);
      if (taskParent == null) {
        // log the error, and continue - this really shouldn't happen since we wrote the message ourselves, but being defensive.
        String msg = LibUtils.getMsg("FILES_TXFR_UNABLE_TO_PARSE_MESSAGE");
        log.error(msg);
        throw new RuntimeException(msg);
      }
      handleMessage(channel, envelope, taskParent);
    } catch (Exception ex) {
      channel.basicNack(envelope.getDeliveryTag(), false, false);
      if (taskParent != null) {
        doErrorParentStepOne(ex, taskParent);
        String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR1", taskParent.getTenantId(), taskParent.getUsername(),
                "handleDelivery", taskParent.getId(), taskParent.getTag(), taskParent.getUuid(), ex.getMessage());
        log.error(msg, ex);

        throw new RuntimeException(msg, ex);
      } else {
        log.error("Unable to create parent task opject from RabbitMQ message", ex);
      }
    }

  }

  public void handleMessage(Channel channel, Envelope envelope, TransferTaskParent taskParent) throws IOException {
    int retry = 0;
    Exception lastException = null;
    while (retry < maxRetries) {
      try {
        if(isLocalMove(taskParent.getTransferType())) {
          doLocalMove(taskParent);
          channel.basicAck(envelope.getDeliveryTag(), false);
          return;
        } else {
          if (createChildTasks(taskParent)) {
            channel.basicAck(envelope.getDeliveryTag(), false);
            return;
          }
        }
      } catch (ServiceException ex) {
        lastException = ex;
      } catch (Exception ex) {
        lastException = ex;
        // unexpected exception occurred - don't retry, just fail
        break;
      }

      retry++;
    }

    // out of retries, so give up on this one.
    doErrorParentStepOne(lastException, taskParent);
    channel.basicNack(envelope.getDeliveryTag(), false, false);
  }

  private boolean isLocalMove(TransferTaskParent.TransferType transferType) {
    if(TransferTaskParent.TransferType.SERVICE_MOVE_DIRECTORY_CONTENTS.equals(transferType) ||
            (TransferTaskParent.TransferType.SERVICE_MOVE_FILE_OR_DIRECTORY.equals(transferType))) {
      return true;
    }

    return false;
  }

  public void doLocalMove(TransferTaskParent taskParent) throws IOException, ServiceException {
    TransferURI sourceUri = taskParent.getSourceURI();
    TransferURI destinationUri = taskParent.getDestinationURI();

    String systemId = sourceUri.getSystemId();
    String srcPathStr = sourceUri.getPath();
    String dstPathStr = destinationUri.getPath();
    String srcRelPathStr = PathUtils.getRelativePath(srcPathStr).toString();
    String dstRelPathStr = PathUtils.getRelativePath(dstPathStr).toString();
    String tenant = taskParent.getTenantId();
    String user = taskParent.getUsername();

    if(!StringUtils.equals(systemId, destinationUri.getSystemId())) {
      String msg = MsgUtils.getMsg("FILES_OPSC_DTN_MOVE_INVALID", tenant, user, taskParent.getTaskId(), "Source and destinatation are not the same");
      log.error(msg);
      throw new IOException(msg);
    }

    if((!sourceUri.isTapisProtocol()) || (!destinationUri.isTapisProtocol())) {
      String msg = MsgUtils.getMsg("FILES_OPSC_DTN_MOVE_INVALID", tenant, user, taskParent.getTaskId(), "Tapis is the only supported protocol");
      log.error(msg);
      throw new IOException(msg);
    }

    // Reserve a client connection, use it to perform the operation and then release it
    IRemoteDataClient client = null;
    try
    {
      TapisSystem system = systemsCache.getSystem(tenant, systemId, user);
      client = remoteDataClientFactory.getRemoteDataClient(tenant, user, system);
      FileOpsService.MoveCopyOperation moveOp =
              TransferTaskParent.TransferType.SERVICE_MOVE_FILE_OR_DIRECTORY.equals(taskParent.getTransferType()) ?
              FileOpsService.MoveCopyOperation.SERVICE_MOVE_FILE_OR_DIRECTORY :
              FileOpsService.MoveCopyOperation.SERVICE_MOVE_DIRECTORY_CONTENTS;
      fileOpsService.moveOrCopy(client, moveOp, srcRelPathStr, dstRelPathStr);
      updateTaskMoveSuccess(taskParent);
    }
    catch (Exception ex)
    {
      String msg = LibUtils.getMsg("FILES_OPSC_ERR", tenant, user, "dtnMove", systemId, srcRelPathStr, ex.getMessage());
      log.error(msg, ex);
      updateTaskFailure(taskParent, ex);
      throw new WebApplicationException(msg, ex);
    }
  }

  /* *********************************************************************** */
  /*            Private Methods                                              */
  /* *********************************************************************** */

  boolean createChildTasks(TransferTaskParent parentTask) throws ServiceException {
    log.debug("***** Starting Parent Task ****");
    log.debug(parentTask.toString());

    // used to return a parent task.
    if (!updateTopTask(parentTask)) {
      return false;
    }

    try {
      if (parentTask.getSourceURI().isTapisProtocol()) {
        handleTapisTransfer(parentTask);
      } else {
        handleNonTapisTransfer(parentTask);
      }
    } catch (DAOException | TapisException | IOException e) {
      String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR1", parentTask.getTaskId(), parentTask.getUsername(), "doParentStepOneB",
              parentTask.getId(), parentTask.getTag(), parentTask.getUuid(), e.getMessage());
      log.error(msg, e);
      throw new ServiceException(msg, e);
    }

    return true;
  }

  private void updateTaskMoveSuccess(TransferTaskParent parentTask) throws DAOException {
    int topTaskId = parentTask.getTaskId();
    TransferTask topTask = dao.getTransferTaskByID(topTaskId);
    // Check to see if all children of a parent task are complete. If so, update the parent task.
    if (!parentTask.getStatus().equals(TransferTaskStatus.COMPLETED)) {
      parentTask.setStatus(TransferTaskStatus.COMPLETED);
      parentTask.setEndTime(Instant.now());
      parentTask.setFinalMessage("Completed");

      log.trace(LibUtils.getMsg("FILES_TXFR_TASK_MOVE_COMPLETE", topTaskId, topTask.getUuid(), parentTask.getId(), parentTask.getUuid(), parentTask.getTag()));
      dao.updateTransferTaskParent(parentTask);
    }
    // Check to see if all the children of a top task are complete. If so, update the top task.
    if (!topTask.getStatus().equals(TransferTaskStatus.COMPLETED)) {
      long incompleteParentCount = dao.getIncompleteParentCount(topTaskId);
      long incompleteChildCount = dao.getIncompleteChildrenCount(topTaskId);
      if (incompleteChildCount == 0 && incompleteParentCount == 0) {
        topTask.setStatus(TransferTaskStatus.COMPLETED);
        topTask.setEndTime(Instant.now());

        log.trace(LibUtils.getMsg("FILES_TXFR_PARENT_TASK_MOVE_COMPLETE", topTaskId, topTask.getUuid(), topTask.getTag()));
        dao.updateTransferTask(topTask);
      }
    }
  }

  private void updateTaskFailure(TransferTaskParent parentTask, Exception cause) {
    log.error(LibUtils.getMsg("FILES_TXFR_SVC_MOVE_ERROR", parentTask.toString(), parentTask.getTaskId()));

    // Update the parent task
    try {
      parentTask = dao.updateTransferTaskParent(parentTask);

      // Mark FAILED_OPT or FAILED and set error message
      // NOTE: We can have a child which is required but the parent is optional
      if (parentTask.isOptional()) {
        parentTask.setStatus(TransferTaskStatus.FAILED_OPT);
      } else {
        parentTask.setStatus(TransferTaskStatus.FAILED);
      }
      parentTask.setEndTime(Instant.now());
      parentTask.setErrorMessage(cause.getMessage());
      parentTask.setFinalMessage("Failed - Child doErrorStepOne");

      log.error(LibUtils.getMsg("FILES_TXFR_SVC_PARENT_FAILED_MOVE", parentTask.getId(), parentTask.getTag(), parentTask.getUuid(), parentTask.getStatus()));
      dao.updateTransferTaskParent(parentTask);
      // If parent is required update top level task to FAILED and set error message
      TransferTask topTask = dao.getTransferTaskByID(parentTask.getTaskId());
      if (parentTask.isOptional()) {
        topTask.setStatus(TransferTaskStatus.FAILED);
        topTask.setErrorMessage(cause.getMessage());
        topTask.setEndTime(Instant.now());
      } else {
        topTask.setStatus(TransferTaskStatus.FAILED);
        topTask.setErrorMessage(cause.getMessage());
        topTask.setEndTime(Instant.now());
      }

      log.error(LibUtils.getMsg("FILES_TXFR_SVC_MOVE_FAILURE", topTask.getId(), topTask.getTag(), topTask.getUuid(), parentTask.getId(), parentTask.getUuid()));
      dao.updateTransferTask(topTask);

    } catch (DAOException ex) {
      log.error(LibUtils.getMsg("FILES_TXFR_SVC_UPDATE_STATUS_ERROR", parentTask.getTenantId(), parentTask.getUsername(),
              "move error", parentTask.getId(), parentTask.getTag(), parentTask.getUuid(), ex.getMessage()), ex);
    }
  }

  /**
   * Return true if we should continue with created tasks, or false if not.  Throw exception on error.
   *
   * @param parentTask
   * @return
   */
  private boolean updateTopTask(TransferTaskParent parentTask) throws ServiceException {
    log.debug("*****  Update Top Task  ****");
    log.debug(parentTask.toString());

    String taskTenant = parentTask.getTenantId();
    String taskUser = parentTask.getUsername();
    String tag = parentTask.getTag();
    Integer parentId = parentTask.getId();
    Integer topTaskId = parentTask.getTaskId();
    UUID parentUuid = parentTask.getUuid();

    // Update the top level task and then the parent task
    try {
      // Update top level task.
      TransferTask topTask = dao.getTransferTaskByID(topTaskId);
      if (topTask.getStartTime() == null) {
        log.trace(LibUtils.getMsg("FILES_TXFR_TASK_START", taskTenant, taskUser, "doParentStepOneA04", topTask.getId(), parentId, parentUuid, tag));
        topTask.setStartTime(Instant.now());
        // Update status unless already in a terminal state (such as cancelled)
        if (!topTask.isTerminal()) topTask.setStatus(TransferTaskStatus.IN_PROGRESS);
        dao.updateTransferTask(topTask);
      }

      // If top task in terminal state then return
      if (topTask.isTerminal()) {
        log.trace(LibUtils.getMsg("FILES_TXFR_TOP_TASK_TERM", taskTenant, taskUser, "doParentStepOneA03", topTaskId, topTask.getStatus(), parentId, parentTask.getStatus(), parentUuid, tag));
        topTask.setEndTime(Instant.now());
        dao.updateTransferTask(topTask);
        return false;
      }

      // Update parent task
      log.trace(LibUtils.getMsg("FILES_TXFR_PARENT_START", taskTenant, taskUser, "doParentStepOneA05", parentId, parentUuid, tag));
      parentTask.setStartTime(Instant.now());
      // Update status unless already in a terminal state (such as cancelled)
      if (!parentTask.isTerminal()) parentTask.setStatus(TransferTaskStatus.IN_PROGRESS);
      parentTask = dao.updateTransferTaskParent(parentTask);

      // If already in a terminal state then set end time and return
      if (parentTask.isTerminal()) {
        log.warn(LibUtils.getMsg("FILES_TXFR_PARENT_TERM", taskTenant, taskUser, "doParentStepOneA01", topTaskId, parentId, parentTask.getStatus(), parentUuid, tag));
        parentTask.setEndTime(Instant.now());
        parentTask.setFinalMessage(LibUtils.getMsg("FILES_TXFR_PARENT_END_TERM", tag, parentTask.getStatus()));
        dao.updateTransferTaskParent(parentTask);
        return false;
      }
    } catch (DAOException ex) {
      String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR1", taskTenant, taskUser, "doParentStepOneA06",
              parentId, tag, parentUuid, ex.getMessage());
      log.error(msg, ex);
      throw new ServiceException(msg, ex);
    }

    return true;
  }

  private void handleTapisTransfer(TransferTaskParent parentTask) throws ServiceException, TapisException, DAOException, IOException {
    TransferURI srcUri = parentTask.getSourceURI();
    TransferURI dstUri = parentTask.getDestinationURI();
    TapisSystem srcSystem, dstSystem;
    IRemoteDataClient srcClient;
    String srcId = srcUri.getSystemId();
    String srcPath = srcUri.getPath();
    String dstId = dstUri.getSystemId();
    String srcSharedCtxGrantor = parentTask.getSrcSharedCtxGrantor();
    String dstSharedCtxGrantor = parentTask.getDestSharedCtxGrantor();
    String impersonationIdNull = null;
    String taskTenant = parentTask.getTenantId();
    String taskUser = parentTask.getUsername();
    String tag = parentTask.getTag();
    Integer parentId = parentTask.getId();
    UUID parentUuid = parentTask.getUuid();

    AuthenticatedUser aUser = new AuthenticatedUser(taskUser, taskTenant, TapisThreadContext.AccountType.user.name(),
            null, taskUser, taskTenant, null, null, null);
    ResourceRequestUser rUser = new ResourceRequestUser(aUser);

    // Handle protocol tapis://
    // Destination must also be tapis protocol. Checked early on, but check again in case it slipped through.
    if (!dstUri.isTapisProtocol()) {
      String msg = LibUtils.getMsg("FILES_TXFR_DST_NOTSUPPORTED", srcUri, dstUri, tag);
      log.error(msg);
      throw new ServiceException(msg);
    }
    // Fetch systems, they are needed during child task creation. Also, this ensures they exist and are available
    srcSystem = LibUtils.getSystemIfEnabled(rUser, systemsCache, srcId, impersonationIdNull, srcSharedCtxGrantor);
    dstSystem = LibUtils.getSystemIfEnabled(rUser, systemsCache, dstId, impersonationIdNull, dstSharedCtxGrantor);
    boolean srcIsS3 = SystemTypeEnum.S3.equals(srcSystem.getSystemType());
    boolean srcIsGlobus = SystemTypeEnum.GLOBUS.equals(srcSystem.getSystemType());
    boolean dstIsS3 = SystemTypeEnum.S3.equals(dstSystem.getSystemType());

    // Establish client
    srcClient = remoteDataClientFactory.getRemoteDataClient(taskTenant, taskUser, srcSystem);

    // Check that src path exists. If not found it is an error.
    FileInfo fileInfo = srcClient.getFileInfo(srcPath, true);
    if (fileInfo == null) {
      String msg = LibUtils.getMsg("FILES_TXFR_SVC_SRCPATH_NOTFOUND", taskTenant, taskUser, parentId, parentUuid, srcPath, tag);
      log.error(msg);
      throw new ServiceException(msg);
    }

    // Get a listing of all files to be transferred
    //TODO: Retries will break this, should delete anything in the DB if it is a retry?
    List<FileInfo> fileListing;
    // NOTE Treat all source system types the same. For S3 it will be all objects matching the srcPath as a prefix.
    log.trace(LibUtils.getMsg("FILES_TXFR_LSR1", taskTenant, taskUser, "doParentStepOneA07", parentId, parentUuid, srcId, srcPath, tag));
    fileListing = fileOpsService.lsRecursive(srcClient, srcPath, false, FileOpsService.MAX_RECURSION, IRemoteDataClient.NO_PATTERN);
    if (fileListing == null) fileListing = Collections.emptyList();
    log.trace(LibUtils.getMsg("FILES_TXFR_LSR2", taskTenant, taskUser, "doParentStepOneA08", parentId, parentUuid, srcId, srcPath, fileListing.size(), tag));

    // If no items to transfer then no child tasks, so we are done.
    // In theory this should be very unlikely since we just checked that source path exists.
    // In practice, it could happen if source path is deleted around the same time.
    // Also, in practice it has happened due to listing improperly returning an empty list.
    // If we do not handle it here we can end up with tasks stuck in the IN_PROGRESS state.
    if (fileListing.isEmpty()) {
      parentTask.setEndTime(Instant.now());
      parentTask.setStatus(TransferTaskStatus.COMPLETED);
      parentTask.setFinalMessage(LibUtils.getMsg("FILES_TXFR_PARENT_COMPLETE_NO_ITEMS", srcId, srcPath, tag));
      dao.updateTransferTaskParent(parentTask);
      return;
    }

    // Create child tasks for each file or object to be transferred.
    List<TransferTaskChild> children = new ArrayList<>();
    long totalBytes = 0;
    for (FileInfo f : fileListing) {
      log.trace(LibUtils.getMsg("FILES_TXFR_ADD_CHILD1", taskTenant, taskUser, "doParentStepOneA09", parentId, parentUuid, f, tag));
      // If destination is of type S3 we skip directories
      if (dstIsS3 && f.isDir()) {
        log.trace(LibUtils.getMsg("FILES_TXFR_SKIP_DIR", taskTenant, taskUser, "doParentStepOneA10", parentId, parentUuid, srcId, srcPath, dstId, f.getPath(), tag));
        continue;
      }
      // Only include the bytes from entries that are not directories. Posix folders are --usually-- 4bytes but not always, so
      // it can make some weird totals that don't really make sense.
      if (!f.isDir()) totalBytes += f.getSize();
      TransferTaskChild child = new TransferTaskChild(parentTask, f, srcSystem);
      children.add(child);
      log.trace(LibUtils.getMsg("FILES_TXFR_ADD_CHILD2", taskTenant, taskUser, "doParentStepOneA11", parentId, parentUuid, child, tag));
    }
    // Update parent task status and totalBytes to be transferred
    log.trace(LibUtils.getMsg("FILES_TXFR_PARENT_STAGE", taskTenant, taskUser, "doParentStepOneA12", parentId, parentUuid, totalBytes, tag));
    parentTask.setTotalBytes(totalBytes);
    parentTask.setStatus(TransferTaskStatus.STAGED);
    parentTask = dao.updateTransferTaskParent(parentTask);
    dao.bulkInsertChildTasks(children);
    children = dao.getAllChildren(parentTask);
    transfersService.publishBulkChildMessages(children);
  }

  private void handleNonTapisTransfer(TransferTaskParent parentTask) throws ServiceException, DAOException {
    // Handle all non-tapis protocols. These are http:// and https://
    // Create a single child task and update parent task status
    TransferTaskChild task = new TransferTaskChild();
    task.setTag(parentTask.getTag());
    task.setSourceURI(parentTask.getSourceURI());
    task.setParentTaskId(parentTask.getId());
    task.setTaskId(parentTask.getTaskId());
    task.setDestinationURI(parentTask.getDestinationURI());
    task.setStatus(TransferTaskStatus.ACCEPTED);
    task.setTenantId(parentTask.getTenantId());
    task.setUsername(parentTask.getUsername());
    task = dao.insertChildTask(task);
    transfersService.publishChildMessage(task);
    parentTask.setStatus(TransferTaskStatus.STAGED);
    dao.updateTransferTaskParent(parentTask);
  }


  /**
   * This method handles exceptions/errors if the parent task failed.
   * A parent task may have no children, so we also need to check for completion of top level task.
   *
   * @param caughtException Exception
   * @param parent          TransferTaskParent
   * @return TransferTasksParent TransferTaskParent
   */
  private TransferTaskParent doErrorParentStepOne(Exception caughtException, TransferTaskParent parent) {
    try {
      log.error(LibUtils.getMsg("FILES_TXFR_SVC_ERR7A", parent.toString(), caughtException));

      // First update parent task, mark FAILED_OPT or FAILED
      if (parent.isOptional())
        parent.setStatus(TransferTaskStatus.FAILED_OPT);
      else
        parent.setStatus(TransferTaskStatus.FAILED);
      parent.setEndTime(Instant.now());
      parent.setErrorMessage(caughtException.getMessage());
      parent.setFinalMessage("Failed - doErrorParentStepOne");
      parent = dao.updateTransferTaskParent(parent);
      // This should really never happen, it means that the parent with that ID was not in the database.
      if (parent == null) {
        return null;
      }

      // Now update the top level task
      TransferTask topTask = dao.getTransferTaskByID(parent.getTaskId());
      // This should also not happen, it means that the top task was not in the database.
      if (topTask == null) {
        return null;
      }

      // If parent is optional we need to check to see if top task status should be updated
      // else parent is required so update top level task to FAILED
      if (parent.isOptional()) {
        checkForComplete(topTask.getId());
      } else {
        topTask.setStatus(TransferTaskStatus.FAILED);
        topTask.setEndTime(Instant.now());
        topTask.setErrorMessage(caughtException.getMessage());
        log.debug(LibUtils.getMsg("FILES_TXFR_SVC_ERR7C", topTask.getId(), topTask.getTag(), topTask.getUuid(), parent.getId(), parent.getUuid()));
        dao.updateTransferTask(topTask);
      }
    } catch (DAOException ex) {
      log.error(LibUtils.getMsg("FILES_TXFR_SVC_ERR1", parent.getTenantId(), parent.getUsername(),
              "doParentErrorStepOne", parent.getId(), parent.getTag(), parent.getUuid(), ex.getMessage()), ex);
    }
    return parent;
  }

  /**
   * Check to see if the top level TransferTask should be marked as finished.
   * If yes then update status.
   *
   * @param topTaskId ID of top level TransferTask
   */
  private void checkForComplete(int topTaskId) throws DAOException {
    TransferTask topTask = dao.getTransferTaskByID(topTaskId);
    // Check to see if all the children of a top task are complete. If so, update the top task.
    if (!topTask.getStatus().equals(TransferTaskStatus.COMPLETED)) {
      long incompleteParentCount = dao.getIncompleteParentCount(topTaskId);
      long incompleteChildCount = dao.getIncompleteChildrenCount(topTaskId);
      if (incompleteChildCount == 0 && incompleteParentCount == 0) {
        topTask.setStatus(TransferTaskStatus.COMPLETED);
        topTask.setEndTime(Instant.now());
        log.trace(LibUtils.getMsg("FILES_TXFR_TASK_COMPLETE1", topTaskId, topTask.getUuid(), topTask.getTag()));
        dao.updateTransferTask(topTask);
      }
    }
  }
}
