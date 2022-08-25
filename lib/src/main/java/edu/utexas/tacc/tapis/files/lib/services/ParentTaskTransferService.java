package edu.utexas.tacc.tapis.files.lib.services;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.ForbiddenException;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.AcknowledgableDelivery;
import reactor.util.retry.Retry;

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
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

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

    @Inject
    public ParentTaskTransferService(TransfersService transfersService,
                                     FileTransfersDAO dao,
                                     FileOpsService fileOpsService,
                                     FilePermsService permsService,
                                     RemoteDataClientFactory remoteDataClientFactory,
                                     SystemsCache systemsCache)
    {
        this.transfersService = transfersService;
        this.dao = dao;
        this.fileOpsService = fileOpsService;
        this.systemsCache = systemsCache;
        this.remoteDataClientFactory = remoteDataClientFactory;
        this.permsService = permsService;
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
   * The one and only step for a ParentTask
   *
   * We prepare a "bill of materials" for the total transfer task.
   * This includes doing a recursive listing and inserting the records into the DB, then publishing all the messages to
   * rabbitmq. After that, the child task workers will pick them up and begin the actual transferring of bytes.
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
    TapisSystem sourceSystem;
    IRemoteDataClient sourceClient;

    // If already in a terminal state then return
    if (parentTask.isTerminal()) return parentTask;

    // Check permission
    if (!isPermitted(parentTask)) throw new ForbiddenException();

    // Update the top level task first, if it is not already updated with the startTime
    try
    {
      // Update top level task status, start time
      TransferTask task = dao.getTransferTaskByID(parentTask.getTaskId());
      if (task.isTerminal()) return parentTask;
      if (task.getStartTime() == null)
      {
        task.setStartTime(Instant.now());
        task.setStatus(TransferTaskStatus.IN_PROGRESS);
        dao.updateTransferTask(task);
      }
      //update parent task status, start time
      parentTask.setStatus(TransferTaskStatus.IN_PROGRESS);
      parentTask.setStartTime(Instant.now());
      parentTask = dao.updateTransferTaskParent(parentTask);
    }
    catch (DAOException ex)
    {
      throw new ServiceException(LibUtils.getMsg("FILES_TXFR_SVC_ERR1", parentTask.getTenantId(), parentTask.getUsername(),
              "doParentStepOneA", parentTask.getId(), parentTask.getUuid(), ex.getMessage()), ex);
    }

    // Process the source URI
    try
    {
      TransferURI sourceURI = parentTask.getSourceURI();
      if (sourceURI.toString().startsWith("tapis://"))
      {
        // Handle the special scheme tapis://
        // We get a listing from srcSystem so there can be multiple child tasks
        sourceSystem = systemsCache.getSystem(parentTask.getTenantId(), sourceURI.getSystemId(), parentTask.getUsername());
        // If srcSystem not enabled throw an exception
        if (sourceSystem.getEnabled() == null || !sourceSystem.getEnabled())
        {
          String msg = LibUtils.getMsg("FILES_TXFR_SYS_NOTENABLED", parentTask.getTenantId(),
                  parentTask.getUsername(), parentTask.getId(), parentTask.getUuid(), sourceSystem.getId());
          throw new ServiceException(msg);
        }
        sourceClient = remoteDataClientFactory.getRemoteDataClient(parentTask.getTenantId(), parentTask.getUsername(),
                sourceSystem, parentTask.getUsername());

        // Get a listing of all files / objects to be transferred
        //TODO: Retries will break this, should delete anything in the DB if it is a retry?
        List<FileInfo> fileListing = fileOpsService.lsRecursive(sourceClient, sourceURI.getPath(), 10);
        // Create child tasks for each file or object to be transferred.
        List<TransferTaskChild> children = new ArrayList<>();
        long totalBytes = 0;
        // TODO: Is it possible for there to be no children? Do we need to simply update top level task to complete if so?
        for (FileInfo f : fileListing)
        {
          // Only include the bytes from files. Posix folders are --usually-- 4bytes but not always, so
          // it can make some weird totals that don't really make sense.
          if (!f.isDir()) totalBytes += f.getSize();
          TransferTaskChild child = new TransferTaskChild(parentTask, f);
          children.add(child);
        }
        // Update parent task status and totalBytes to be transferred
        parentTask.setTotalBytes(totalBytes);
        parentTask.setStatus(TransferTaskStatus.STAGED);
        parentTask = dao.updateTransferTaskParent(parentTask);
        dao.bulkInsertChildTasks(children);
        children = dao.getAllChildren(parentTask);
        transfersService.publishBulkChildMessages(children);
      }
      else if (sourceURI.toString().startsWith("http://") || sourceURI.toString().startsWith("https://"))
      {
        // Handle scheme http://
        // Create a single child task and update parent task status
        TransferTaskChild task = new TransferTaskChild();
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
        parentTask = dao.updateTransferTaskParent(parentTask);
      }
    }
    catch (DAOException | TapisException | IOException e)
    {
      throw new ServiceException(LibUtils.getMsg("FILES_TXFR_SVC_ERR1", parentTask.getTenantId(), parentTask.getUsername(),
              "doParentStepOneB", parentTask.getId(), parentTask.getUuid(), e.getMessage()), e);
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
    log.error(LibUtils.getMsg("FILES_TXFR_SVC_ERR7", parent.toString()));
    log.error(LibUtils.getMsg("FILES_TXFR_SVC_ERR7", e));
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
      TransferTask task = dao.getTransferTaskByID(parent.getTaskId());
      // This should also not happen, it means that the top task was not in the database.
      if (task == null) return Mono.empty();

      // If parent is optional we need to check to see if top task status should be updated
      // else parent is required so update top level task to FAILED
      if (parent.isOptional())
      {
        checkForComplete(task.getId());
      }
      else
      {
        task.setStatus(TransferTaskStatus.FAILED);
        task.setEndTime(Instant.now());
        task.setErrorMessage(e.getMessage());
        dao.updateTransferTask(task);
      }
    }
    catch (DAOException ex)
    {
      log.error(LibUtils.getMsg("FILES_TXFR_SVC_ERR1", parent.getTenantId(), parent.getUsername(),
              "doParentErrorStepOne", parent.getId(), parent.getUuid(), ex.getMessage()), ex);
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
    boolean srcSharedAppCtx = parentTask.isSrcSharedAppCtx();
    boolean destSharedAppCtx = parentTask.isDestSharedAppCtx();

    // If we have a tapis:// link, have to do the source perms check
    if (!isHttpSource)
    {
      // If sharedAppCtx is true then skip perm check
      if (!srcSharedAppCtx)
      {
        boolean sourcePerms = permsService.isPermitted(tenantId, username, srcSystemId, srcPath, FileInfo.Permission.READ);
        if (!sourcePerms) return false;
      }
    }
    // If sharedAppCtx is true then skip perm check
    if (!destSharedAppCtx)
    {
      return permsService.isPermitted(tenantId, username, destSystemId, destPath, FileInfo.Permission.MODIFY);
    }
    return true;
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
      log.error(msg);
      throw new ServiceException(msg, ex);
    }
  }

  /**
   * Check to see if the top level TransferTask should be marked as finished.
   * If yes then update status.
   *
   * @param topTaskId Id of top level TransferTask
   */
  private void checkForComplete(int topTaskId) throws DAOException
  {
    TransferTask topTask = dao.getTransferTaskByID(topTaskId);
    // Check to see if all the children of a top task are complete. If so, update the top task.
    if (!topTask.getStatus().equals(TransferTaskStatus.COMPLETED))
    {
      long incompleteCount = dao.getIncompleteChildrenCount(topTaskId);
      if (incompleteCount == 0)
      {
        topTask.setStatus(TransferTaskStatus.COMPLETED);
        topTask.setEndTime(Instant.now());
        dao.updateTransferTask(topTask);
      }
    }
  }
}
