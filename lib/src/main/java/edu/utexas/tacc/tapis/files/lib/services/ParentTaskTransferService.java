package edu.utexas.tacc.tapis.files.lib.services;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import javax.inject.Inject;
import javax.ws.rs.ForbiddenException;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.AcknowledgableDelivery;
import reactor.util.retry.Retry;

import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.json.TapisObjectMapper;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskParent;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import edu.utexas.tacc.tapis.files.lib.models.TransferURI;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;

@Service
public class ParentTaskTransferService
{
  private static final int MAX_RETRIES = 5;
  private final TransfersService transfersService;
  private final FileTransfersDAO dao;
  private static final ObjectMapper mapper = TapisObjectMapper.getMapper();
  private final RemoteDataClientFactory remoteDataClientFactory;
  private final FilePermsService permsService;
  private final SystemsCache systemsCache;
  private final FileOpsService fileOpsService;
  private static final Logger log = LoggerFactory.getLogger(ParentTaskTransferService.class);

  /* *********************************************************************** */
  /*            Constructors                                                 */
  /* *********************************************************************** */

  /*
   * Constructor for service.
   * Note that this is never invoked explicitly. Arguments of constructor are initialized via Dependency Injection.
   */
  @Inject
  public ParentTaskTransferService(TransfersService transfersService1,
                                   FileTransfersDAO dao1,
                                   FileOpsService fileOpsService1,
                                   FilePermsService permsService1,
                                   RemoteDataClientFactory remoteDataClientFactory1,
                                   SystemsCache systemsCache1)
  {
    transfersService = transfersService1;
    dao = dao1;
    fileOpsService = fileOpsService1;
    systemsCache = systemsCache1;
    remoteDataClientFactory = remoteDataClientFactory1;
    permsService = permsService1;
  }

  /* *********************************************************************** */
  /*                      Public Methods                                     */
  /* *********************************************************************** */

  /**
   * Run a full transfer pipeline for a parent task
   *
   * @return a flux for a parent transfer task
   */
  public Flux<TransferTaskParent> runPipeline()
  {
    return transfersService.streamParentMessages()
      .groupBy(m -> { try { return groupByTenant(m); } catch (ServiceException ex) { return Mono.empty(); } } )
      .flatMap(group ->
        {
          Scheduler scheduler = Schedulers.newBoundedElastic(5, 10, "ParentPool:" + group.key());
          return group.flatMap(m ->
            deserializeParentMessage(m)
              .flatMap(t1 -> Mono.fromCallable(() -> doParentStepOne(t1))
                                 .publishOn(scheduler)
                                 .retryWhen(Retry.backoff(MAX_RETRIES, Duration.ofSeconds(1))
                                                 .maxBackoff(Duration.ofMinutes(60))
                                                 .filter(e -> e.getClass() == IOException.class) )
                                 .onErrorResume(e -> doErrorParentStepOne(m, e, t1) ) )
              .flatMap(t2 -> { m.ack(); return Mono.just(t2); }) );
          } );
  }

  /* *********************************************************************** */
  /*            Private Methods                                              */
  /* *********************************************************************** */

  /**
   * The one and only step for a ParentTask.
   * This is the first call in the pipeline kicked off in the reactor flow.
   *
   * We prepare a "bill of materials" for the total transfer task.
   * This includes doing a recursive listing and inserting the records into the DB, then publishing all the messages to
   * rabbitmq. After that, the child task workers will pick them up and begin the transfer of data.
   *
   * @param parentTask TransferTaskParent
   * @return Updated task
   * @throws ForbiddenException when permission denied
   * @throws ServiceException When a listing or DAO error occurs
   */
  private TransferTaskParent doParentStepOne(TransferTaskParent parentTask) throws ServiceException, ForbiddenException
  {
    log.debug("***** DOING doParentStepOne ****");
    log.debug(parentTask.toString());

    // Extract some values for convenience and clarity
    String taskTenant = parentTask.getTenantId();
    String taskUser = parentTask.getUsername();
    // Keep these Integer and not int. When int the logger puts in commas which makes it harder to search the log.
    Integer parentId = parentTask.getId();
    Integer topTaskId = parentTask.getTaskId();
    UUID parentUuid = parentTask.getUuid();
    String tag = parentTask.getTag();

    // Update the top level task and then the parent task
    try
    {
      // Update top level task.
      TransferTask topTask = dao.getTransferTaskByID(topTaskId);
      if (topTask.getStartTime() == null)
      {
        log.trace(LibUtils.getMsg("FILES_TXFR_TASK_START", taskTenant, taskUser, "doParentStepOneA04", topTask.getId(), parentId, parentUuid, tag));
        topTask.setStartTime(Instant.now());
        // Update status unless already in a terminal state (such as cancelled)
        if (!topTask.isTerminal()) topTask.setStatus(TransferTaskStatus.IN_PROGRESS);
        dao.updateTransferTask(topTask);
      }

      // If top task in terminal state then return
      if (topTask.isTerminal())
      {
        log.trace(LibUtils.getMsg("FILES_TXFR_TOP_TASK_TERM", taskTenant, taskUser, "doParentStepOneA03", topTaskId, topTask.getStatus(), parentId, parentTask.getStatus(), parentUuid, tag));
        topTask.setEndTime(Instant.now());
        dao.updateTransferTask(topTask);
        return parentTask;
      }

      // Update parent task
      log.trace(LibUtils.getMsg("FILES_TXFR_PARENT_START", taskTenant, taskUser, "doParentStepOneA05", parentId, parentUuid, tag));
      parentTask.setStartTime(Instant.now());
      // Update status unless already in a terminal state (such as cancelled)
      if (!parentTask.isTerminal()) parentTask.setStatus(TransferTaskStatus.IN_PROGRESS);
      parentTask = dao.updateTransferTaskParent(parentTask);

      // Check permission. If not permitted update status to FAILED and throw an exception
      if (!isPermitted(parentTask))
      {
        String msg = LibUtils.getMsg("FILES_TXFR_SVC_PERM", taskTenant, taskUser, "doParentStepOneA02", parentId, parentUuid, tag);
        log.warn(msg);
        if (!parentTask.isTerminal())
        {
          parentTask.setEndTime(Instant.now());
          parentTask.setStatus(TransferTaskStatus.FAILED);
        }
        dao.updateTransferTaskParent(parentTask);
        throw new ForbiddenException(msg);
      }

      // If already in a terminal state then set end time and return
      if (parentTask.isTerminal())
      {
        log.warn(LibUtils.getMsg("FILES_TXFR_PARENT_TERM", taskTenant, taskUser, "doParentStepOneA01", topTaskId, parentId, parentTask.getStatus(), parentUuid, tag));
        parentTask.setEndTime(Instant.now());
        parentTask = dao.updateTransferTaskParent(parentTask);
        return parentTask;
      }
    }
    catch (DAOException ex)
    {
      String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR1", taskTenant, taskUser, "doParentStepOneA06",
                                   parentId, tag, parentUuid, ex.getMessage());
      log.error(msg, ex);
      throw new ServiceException(msg, ex);
    }

    // =======================================================================
    // Now for the main work of creating the child tasks and publishing them
    // =======================================================================
    // Create a ResourceRequestUser. Some library calls use this, mainly as convenient wrapper for logging identity info
    AuthenticatedUser aUser = new AuthenticatedUser(taskUser, taskTenant, TapisThreadContext.AccountType.user.name(),
                                                    null, taskUser, taskTenant, null, null, null);
    ResourceRequestUser rUser = new ResourceRequestUser(aUser);

    // Process the source URI
    TransferURI srcUri = parentTask.getSourceURI();
    TransferURI dstUri = parentTask.getDestinationURI();
    TapisSystem srcSystem, dstSystem;
    IRemoteDataClient srcClient;
    String srcId = srcUri.getSystemId();
    String srcPath = srcUri.getPath();
    String dstId = dstUri.getSystemId();
//TODO sharedCtxGrantor
    String srcSharedCtxGrantor = Boolean.toString(parentTask.isSrcSharedAppCtx()); //parentTask.getSrcSharedCtxGrantor();
    String dstSharedCtxGrantor = Boolean.toString(parentTask.isDestSharedAppCtx()); //parentTask.getDestSharedCtxGrantor();
    String impersonationIdNull = null;

    try
    {
      if (srcUri.isTapisProtocol())
      {
        // Handle protocol tapis://
        // Destination must also be tapis protocol. Checked early on, but check again in case it slipped through.
        if (!dstUri.isTapisProtocol())
        {
          String msg = LibUtils.getMsg("FILES_TXFR_DST_NOTSUPPORTED", srcUri, dstUri, tag);
          log.error(msg);
          throw new ServiceException(msg);
        }
        // Fetch systems, they are needed during child task creation. Also, this ensures they exist and are available
        srcSystem = LibUtils.getSystemIfEnabled(rUser, systemsCache, srcId, impersonationIdNull, srcSharedCtxGrantor);
        dstSystem = LibUtils.getSystemIfEnabled(rUser, systemsCache, dstId, impersonationIdNull, dstSharedCtxGrantor);
        boolean srcIsS3 = SystemTypeEnum.S3.equals(srcSystem.getSystemType());
        boolean dstIsS3 = SystemTypeEnum.S3.equals(dstSystem.getSystemType());

        // Establish client
        srcClient = remoteDataClientFactory.getRemoteDataClient(taskTenant, taskUser, srcSystem);

        // Check that src path exists. If not found it is an error.
        FileInfo fileInfo = srcClient.getFileInfo(srcPath);
        if (fileInfo == null)
        {
          String msg = LibUtils.getMsg("FILES_TXFR_SVC_SRCPATH_NOTFOUND", taskTenant, taskUser, parentId, parentUuid, srcPath, tag);
          log.error(msg);
          throw new ServiceException(msg);
        }

        // Get a listing of all files to be transferred
        //TODO: Retries will break this, should delete anything in the DB if it is a retry?
        List<FileInfo> fileListing;
        // NOTE Treat all source system types the same. For S3 it will be all objects matching the srcPath as a prefix.
        log.trace(LibUtils.getMsg("FILES_TXFR_LSR1", taskTenant, taskUser, "doParentStepOneA07", parentId, parentUuid, srcId, srcPath, tag));
        fileListing = fileOpsService.lsRecursive(srcClient, srcPath, FileOpsService.MAX_RECURSION);
        if (fileListing == null) fileListing = Collections.emptyList();
        log.trace(LibUtils.getMsg("FILES_TXFR_LSR2", taskTenant, taskUser, "doParentStepOneA08", parentId, parentUuid, srcId, srcPath, fileListing.size(), tag));

        // Create child tasks for each file or object to be transferred.
        List<TransferTaskChild> children = new ArrayList<>();
        long totalBytes = 0;
        for (FileInfo f : fileListing)
        {
          log.trace(LibUtils.getMsg("FILES_TXFR_ADD_CHILD1", taskTenant, taskUser, "doParentStepOneA09", parentId, parentUuid, f, tag));
          // If destination is of type S3 we skip directories
          if (dstIsS3 && f.isDir())
          {
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
      else
      {
        // Handle all non-tapis protocols. These are http:// and https://
        // Create a single child task and update parent task status
        TransferTaskChild task = new TransferTaskChild();
        task.setTag(tag);
        task.setSourceURI(parentTask.getSourceURI());
        task.setParentTaskId(parentId);
        task.setTaskId(topTaskId);
        task.setDestinationURI(parentTask.getDestinationURI());
        task.setStatus(TransferTaskStatus.ACCEPTED);
        task.setTenantId(parentTask.getTenantId());
        task.setUsername(parentTask.getUsername());
        task = dao.insertChildTask(task);
        transfersService.publishChildMessage(task);
        parentTask.setStatus(TransferTaskStatus.STAGED);
        parentTask = dao.updateTransferTaskParent(parentTask);
      }
    }
    catch (DAOException | TapisException | IOException e)
    {
      String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR1", taskTenant, taskUser, "doParentStepOneB",
                                   parentId, tag, parentUuid, e.getMessage());
      log.error(msg, e);
      throw new ServiceException(msg, e);
    }
    return parentTask;
  }

  /**
   * This method handles exceptions/errors if the parent task failed.
   * A parent task may have no children, so we also need to check for completion of top level task.
   *
   * @param m      message from rabbitmq
   * @param e      Throwable
   * @param parent TransferTaskParent
   * @return Mono TransferTaskParent
   */
  private Mono<TransferTaskParent> doErrorParentStepOne(AcknowledgableDelivery m, Throwable e, TransferTaskParent parent)
  {
    log.error(LibUtils.getMsg("FILES_TXFR_SVC_ERR7A", parent.toString()));
    log.error(LibUtils.getMsg("FILES_TXFR_SVC_ERR7B", e.getMessage()));
    m.nack(false);

    // First update parent task, mark FAILED_OPT or FAILED
    if (parent.isOptional())
      parent.setStatus(TransferTaskStatus.FAILED_OPT);
    else
      parent.setStatus(TransferTaskStatus.FAILED);
    parent.setEndTime(Instant.now());
    parent.setErrorMessage(e.getMessage());
    try
    {
      parent = dao.updateTransferTaskParent(parent);
      // This should really never happen, it means that the parent with that ID was not in the database.
      if (parent == null) return Mono.empty();

      // Now update the top level task
      TransferTask topTask = dao.getTransferTaskByID(parent.getTaskId());
      // This should also not happen, it means that the top task was not in the database.
      if (topTask == null) return Mono.empty();

      // If parent is optional we need to check to see if top task status should be updated
      // else parent is required so update top level task to FAILED
      if (parent.isOptional())
      {
        checkForComplete(topTask.getId());
      }
      else
      {
        topTask.setStatus(TransferTaskStatus.FAILED);
        topTask.setEndTime(Instant.now());
        topTask.setErrorMessage(e.getMessage());
        log.error(LibUtils.getMsg("FILES_TXFR_SVC_ERR7C", topTask.getId(), topTask.getTag(), topTask.getUuid(), parent.getId(), parent.getUuid()));
        dao.updateTransferTask(topTask);
      }
    }
    catch (DAOException ex)
    {
      log.error(LibUtils.getMsg("FILES_TXFR_SVC_ERR1", parent.getTenantId(), parent.getUsername(),
              "doParentErrorStepOne", parent.getId(), parent.getTag(), parent.getUuid(), ex.getMessage()), ex);
    }
    return Mono.just(parent);
  }

  /**
   * This method checks the permissions on both the source and destination of the transfer.
   *
   * @param parentTask the TransferTaskParent
   * @return boolean is/is not permitted.
   * @throws ServiceException When api calls for permissions fail
   */
  private boolean isPermitted(TransferTaskParent parentTask) throws ServiceException
  {
    // For http inputs no need to do any permission checking on the source
    boolean isHttpSource = parentTask.getSourceURI().getProtocol().equalsIgnoreCase("http");
    String tenantId = parentTask.getTenantId();
    String username = parentTask.getUsername();

    String srcSystemId = parentTask.getSourceURI().getSystemId();
    String srcPath = parentTask.getSourceURI().getPath();
    String destSystemId = parentTask.getDestinationURI().getSystemId();
    String destPath = parentTask.getDestinationURI().getPath();
// TODO sharedCtxGrantor
    String srcSharedAppCtx = null; //parentTask.getSrcSharedCtxGrantor();
    String destSharedAppCtx = null; //parentTask.getDestSharedCtxGrantor();

    // Do source path perms check if it is not http/s
    if (!isHttpSource)
    {
// TODO Update for sharing
//      // If sharedAppCtx is true then skip perm check
//      if (!srcSharedAppCtx)
//      {

      // TODO
      // TODO For now if sharedCtxGrantor set to anything other than false then allow
      // TODO
      boolean sharedCtx = !StringUtils.isBlank(srcSharedAppCtx);
      if (sharedCtx && !"FALSE".equalsIgnoreCase(srcSharedAppCtx)) return true;
      // TODO

        boolean sourcePerms = permsService.isPermitted(tenantId, username, srcSystemId, srcPath, FileInfo.Permission.READ);
        if (!sourcePerms) return false;
//      }
    }

    // Do target path perms check
// TODO Update for sharing
//    // If sharedAppCtx is true then skip perm check
//    if (!destSharedAppCtx)
//    {

    // TODO
    // TODO For now if sharedCtxGrantor set to anything other than false then allow
    // TODO
    boolean sharedCtx = !StringUtils.isBlank(destSharedAppCtx);
    if (sharedCtx && !"FALSE".equalsIgnoreCase(destSharedAppCtx)) return true;
    // TODO

    return permsService.isPermitted(tenantId, username, destSystemId, destPath, FileInfo.Permission.MODIFY);
//    }
//    return true;
  }

  private Mono<TransferTaskParent> deserializeParentMessage(AcknowledgableDelivery message)
  {
    try
    {
      TransferTaskParent parent = mapper.readValue(message.getBody(), TransferTaskParent.class);
      return Mono.just(parent);
    }
    catch (IOException ex)
    {
      // DO NOT requeue the message if it fails here!
      message.nack(false);
      return Mono.empty();
    }
  }

  private String groupByTenant(AcknowledgableDelivery message) throws ServiceException
  {
    try
    {
      return mapper.readValue(message.getBody(), TransferTaskParent.class).getTenantId();
    }
    catch (IOException ex)
    {
      message.nack(false);
      String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR11", ex.getMessage());
      log.error(msg, ex);
      throw new ServiceException(msg, ex);
    }
  }

  /**
   * Check to see if the top level TransferTask should be marked as finished.
   * If yes then update status.
   *
   * @param topTaskId ID of top level TransferTask
   */
  private void checkForComplete(int topTaskId) throws DAOException
  {
    TransferTask topTask = dao.getTransferTaskByID(topTaskId);
    // Check to see if all the children of a top task are complete. If so, update the top task.
    if (!topTask.getStatus().equals(TransferTaskStatus.COMPLETED))
    {
      long incompleteParentCount = dao.getIncompleteParentCount(topTaskId);
      long incompleteChildCount = dao.getIncompleteChildrenCount(topTaskId);
      if (incompleteChildCount == 0 && incompleteParentCount == 0)
      {
        topTask.setStatus(TransferTaskStatus.COMPLETED);
        topTask.setEndTime(Instant.now());
        log.trace(LibUtils.getMsg("FILES_TXFR_TASK_COMPLETE1", topTaskId, topTask.getUuid(), topTask.getTag()));
        dao.updateTransferTask(topTask);
      }
    }
  }
}
