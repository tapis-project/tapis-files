package edu.utexas.tacc.tapis.files.lib.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.clients.HTTPClient;
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
import edu.utexas.tacc.tapis.files.lib.transfers.ObservableInputStream;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.jetbrains.annotations.NotNull;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.AcknowledgableDelivery;
import reactor.util.retry.Retry;

import javax.inject.Inject;
import javax.ws.rs.NotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Service
public class ChildTaskTransferService
{
    private static final int MAX_RETRIES = 5;
    private final TransfersService transfersService;
    private final FileTransfersDAO dao;
    private static final ObjectMapper mapper = TapisObjectMapper.getMapper();
    private final RemoteDataClientFactory remoteDataClientFactory;
    private final SystemsCache systemsCache;
    private final FileUtilsService fileUtilsService;
    private static final Logger log = LoggerFactory.getLogger(ChildTaskTransferService.class);

  /* *********************************************************************** */
  /*            Constructors                                                 */
  /* *********************************************************************** */

  @Inject
  public ChildTaskTransferService(TransfersService transfersService1, FileTransfersDAO dao1,
                                  FileUtilsService fileUtilsService1,
                                  RemoteDataClientFactory remoteDataClientFactory1,
                                  SystemsCache systemsCache1)
  {
    transfersService = transfersService1;
    dao = dao1;
    systemsCache = systemsCache1;
    remoteDataClientFactory = remoteDataClientFactory1;
    fileUtilsService = fileUtilsService1;
  }

  /* *********************************************************************** */
  /*                      Public Methods                                     */
  /* *********************************************************************** */

    /**
     * This is the main processing workflow. Starting with the raw message stream created by streamChildMessages(),
     * we walk through the transfer process. A Flux<TransferTaskChild> is returned and can be subscribed to later
     * for further processing / notifications / logging
     *
     * @return a Flux of TransferTaskChild
     */
    public Flux<TransferTaskChild> runPipeline()
    {
        // Deserialize the message so that we can pass that into the reactor chain.
        return transfersService.streamChildMessages()
            .groupBy((m)-> {
                try {
                    return childTaskGrouper(m);
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
                            .flatMap(t1 -> Mono.fromCallable(() -> stepOne(t1))
                                .retryWhen(
                                    Retry.backoff(MAX_RETRIES, Duration.ofMillis(10))
                                        .maxBackoff(Duration.ofSeconds(10))
                                        .scheduler(scheduler)
                                ).onErrorResume(e -> doErrorStepOne(m, e, t1))
                            )
                            .flatMap(t2 -> Mono.fromCallable(() -> doTransfer(t2))
                                .retryWhen(
                                    Retry.backoff( 10, Duration.ofMillis(100))
                                        .maxBackoff(Duration.ofSeconds(120))
                                        .scheduler(scheduler)
                                        .doBeforeRetry(signal-> log.error("RETRY", signal.failure()))
                                        .filter(e -> e.getClass().equals(IOException.class))
                                )
                                .onErrorResume(e -> doErrorStepOne(m, e, t2))
                            )
                            .flatMap(t3 -> Mono.fromCallable(() -> stepThree(t3))
                                .retryWhen(
                                    Retry.backoff(MAX_RETRIES, Duration.ofMillis(10))
                                        .maxBackoff(Duration.ofSeconds(10))
                                        .scheduler(scheduler)
                                ).onErrorResume(e -> doErrorStepOne(m, e, t3))
                            )
                            .flatMap(t4 -> Mono.fromCallable(() -> stepFour(t4))
                                .retryWhen(
                                    Retry.backoff(MAX_RETRIES, Duration.ofMillis(10))
                                        .maxBackoff(Duration.ofSeconds(10))
                                        .scheduler(scheduler)
                                ).onErrorResume(e -> doErrorStepOne(m, e, t4))
                            )
                            .flatMap(t5 -> {
                                m.ack();
                                return Mono.just(t5);
                            })
                            .flatMap(t6 -> Mono.fromCallable(() -> stepFive(t6)))
                    );
            });
    }

  /* *********************************************************************** */
  /*            Private Methods                                              */
  /* *********************************************************************** */

  // ==========================================================================
  // Four major Steps executed during transfer
  //   Step 1: Update TransferTask and ParentTask status
  //   Step 2: Perform the transfer
  //   Step 3: Bookkeeping and cleanup
  //   Step 4: Check for unfinished work (child tasks).
  //           If none then update TransferTask and ParentTask status
  //   Step 5: No-op TBD
  // ==========================================================================

  /**
   * Step one: We update task status and parent task if necessary
   *
   * @param taskChild The child transfer task
   * @return Updated TransferTaskChild
   */
  private TransferTaskChild stepOne(@NotNull TransferTaskChild taskChild) throws ServiceException
  {
    log.info("***** DOING stepOne **** {}", taskChild);
    // Update parent task and then child task
    try
    {
      taskChild = dao.getTransferTaskChild(taskChild.getUuid());
      TransferTaskParent parentTask = dao.getTransferTaskParentById(taskChild.getParentTaskId());
      // If the parent task not in final state and not yet set to IN_PROGRESS do it here.
      if (!parentTask.isTerminal() && !parentTask.getStatus().equals(TransferTaskStatus.IN_PROGRESS))
      {
        parentTask.setStatus(TransferTaskStatus.IN_PROGRESS);
        if (parentTask.getStartTime() == null) parentTask.setStartTime(Instant.now());
        dao.updateTransferTaskParent(parentTask);
      }

      // If cancelled or failed set the end time, and we are done
      if (taskChild.isTerminal())
      {
        taskChild.setEndTime(Instant.now());
        taskChild = dao.updateTransferTaskChild(taskChild);
        return taskChild;
      }

      // Ready to start. Update child task status and start time.
      taskChild.setStatus(TransferTaskStatus.IN_PROGRESS);
      taskChild.setStartTime(Instant.now());
      taskChild = dao.updateTransferTaskChild(taskChild);

      return taskChild;
    }
    catch (DAOException ex)
    {
      String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR1", taskChild.getTenantId(), taskChild.getUsername(),
                                   "stepOne", taskChild.getId(), taskChild.getUuid(), ex.getMessage());
      log.error(msg, ex);
      throw new ServiceException(msg, ex);
    }
  }

  /**
   * Perform the transfer, this is the meat of the operation.
   * NOTE: TBD Support a GLOBUS type point to point transfer
   *
   * @param taskChild the incoming child task
   * @return update child task
   * @throws ServiceException If the DAO updates failed or a transfer failed in flight
   */
  private TransferTaskChild stepTwo(TransferTaskChild taskChild) throws ServiceException, NotFoundException, IOException
  {
    boolean srcIsLinux = false, dstIsLinux = false; // Used for properly handling update of exec perm

    TapisSystem sourceSystem = null;
    TapisSystem destSystem = null;
    IRemoteDataClient sourceClient;
    IRemoteDataClient destClient;
    log.info("***** DOING stepTwo **** {}", taskChild);

    try
    {
      // If cancelled or failed set the end time, and we are done
      if (taskChild.isTerminal())
      {
        taskChild.setEndTime(Instant.now());
        taskChild = dao.updateTransferTaskChild(taskChild);
        return taskChild;
      }

      // Update task in DB to IN_PROGRESS and increment the retries on this particular task
      taskChild.setStatus(TransferTaskStatus.IN_PROGRESS);
      taskChild.setRetries(taskChild.getRetries() + 1);
      taskChild = dao.updateTransferTaskChild(taskChild);
    }
    catch (DAOException ex)
    {
      String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR1", taskChild.getTenantId(), taskChild.getUsername(),
                                   "stepTwoA", taskChild.getId(), taskChild.getUuid(), ex.getMessage());
      log.error(msg, ex);
      throw new ServiceException(msg, ex);
    }

    String sourcePath;
    TransferURI destURL = taskChild.getDestinationURI();
    TransferURI sourceURL = taskChild.getSourceURI();

    // Initialize source path and client
    if (taskChild.getSourceURI().toString().startsWith("https://") || taskChild.getSourceURI().toString().startsWith("http://"))
    {
      // Source path is HTTP/S
      //This should be the full string URL such as http://google.com
      sourcePath = sourceURL.toString();
      sourceClient = new HTTPClient(taskChild.getTenantId(), taskChild.getUsername(), sourceURL.toString(), destURL.toString());
    }
    else
    {
      // Source path is not HTTP/S
      sourcePath = sourceURL.getPath();
      sourceSystem = systemsCache.getSystem(taskChild.getTenantId(), sourceURL.getSystemId(), taskChild.getUsername());
      // If src system is not enabled throw an exception
      if (sourceSystem.getEnabled() == null || !sourceSystem.getEnabled())
      {
        String msg = LibUtils.getMsg("FILES_TXFR_SYS_NOTENABLED", taskChild.getTenantId(),
                taskChild.getUsername(), taskChild.getId(), taskChild.getUuid(), sourceSystem.getId());
        throw new ServiceException(msg);
      }

      // Used for properly handling update of exec perm
      srcIsLinux = SystemTypeEnum.LINUX.equals(sourceSystem.getSystemType());

      sourceClient = remoteDataClientFactory.getRemoteDataClient(taskChild.getTenantId(), taskChild.getUsername(),
              sourceSystem, taskChild.getUsername());
    }

    // Initialize destination client
    try
    {
      destSystem = systemsCache.getSystem(taskChild.getTenantId(), destURL.getSystemId(), taskChild.getUsername());
      // If dst system is not enabled throw an exception
      if (destSystem.getEnabled() == null || !destSystem.getEnabled())
      {
        String msg = LibUtils.getMsg("FILES_TXFR_SYS_NOTENABLED", taskChild.getTenantId(),
                                     taskChild.getUsername(), taskChild.getId(), taskChild.getUuid(), destSystem.getId());
        log.error(msg);
        throw new ServiceException(msg);
      }

      // Used for properly handling update of exec perm
      dstIsLinux = SystemTypeEnum.LINUX.equals(destSystem.getSystemType());

      destClient = remoteDataClientFactory.getRemoteDataClient(taskChild.getTenantId(), taskChild.getUsername(),
                                                               destSystem, taskChild.getUsername());
    }
    catch (IOException | ServiceException ex)
    {
      String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR1", taskChild.getTenantId(), taskChild.getUsername(),
                                   "stepTwoB", taskChild.getId(), taskChild.getUuid(), ex.getMessage());
      log.error(msg, ex);
      throw new ServiceException(msg, ex);
    }

    // If destination is a directory, create the directory and return
    if (taskChild.isDir())
    {
      destClient.mkdir(destURL.getPath());
      return taskChild;
    }

    // NOTE: TBD: Handle a GLOBUS type point to point txfr where we are not in control of the stream.
    //       We will need to initiate the transfer, get the externalTransferId, update the child/parent task
    //         with the externalTransferId (or will this be done in the synchronous call?).
    //       Then monitor the transfer and update the progress.
    // NOTE: TBD: Or, maybe the txfr will be initiated synchronously and all we will need to do here is
    //           use the externalTransferId to monitor the progress and update the status.

    // Stream the file contents to destination. While the InputStream is open,
    // we put a tap on it and send events that get grouped into 100 ms intervals. Progress
    // on the child tasks are updated during the reading of the source input stream.
    try (InputStream sourceStream = sourceClient.getStream(sourcePath);
         ObservableInputStream observableInputStream = new ObservableInputStream(sourceStream) )
    {
      // Observe the progress event stream, just get the last event from the past 1 second.
      final TransferTaskChild finalTaskChild = taskChild;
      observableInputStream.getEventStream()
              .window(Duration.ofMillis(100))
              .flatMap(window -> window.takeLast(1))
              .flatMap((progress) -> updateProgress(progress, finalTaskChild))
              .subscribe();
      destClient.upload(destURL.getPath(), observableInputStream);
    }

    // If it is an executable file on a posix system going to a posix system, chmod it to be +x.
    // Note: sourceSystem will be null and srcIsLinux will be false if source is http/s.
    if (sourceSystem != null && srcIsLinux && dstIsLinux)
    {
      FileInfo item = sourceClient.getFileInfo(sourcePath);
      if (!item.isDir() && item.getNativePermissions().contains("x"))
      {
        try
        {
          // Before calling linuxOp, we need to know if we are in a sharedAppCtx
          // So the linuxOp will skip the perm check
          TransferTaskParent parentTask = dao.getTransferTaskParentById(taskChild.getParentTaskId());
          boolean isDestShared = parentTask.isDestSharedAppCtx();
          boolean recurseFalse = false;
          fileUtilsService.linuxOp(destClient, destURL.getPath(), FileUtilsService.NativeLinuxOperation.CHMOD, "700",
                                   recurseFalse, isDestShared);
        }
        catch (TapisException | DAOException ex)
        {
          String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR1", taskChild.getTenantId(), taskChild.getUsername(),
                                       "chmod", taskChild.getId(), taskChild.getUuid(), ex.getMessage());
          log.error(msg, ex);
          throw new ServiceException(msg, ex);
        }
      }
    }

    //The ChildTransferTask gets updated in another thread, so we look it up again here before passing it on
    try
    {
      taskChild = dao.getTransferTaskChild(taskChild.getUuid());
      return taskChild;
    }
    catch (DAOException ex)
    {
      return null;
    }
  }

  /**
   * Bookkeeping and cleanup. Mark the child as COMPLETE. Update the parent task with the bytes sent
   *
   * @param taskChild The child transfer task
   * @return updated TransferTaskChild
   */
  private TransferTaskChild stepThree(@NotNull TransferTaskChild taskChild) throws ServiceException
  {
    // If it cancelled/failed somehow, just push it through unchanged.
    log.info("***** DOING stepThree **** {}", taskChild);
    try
    {
      // If we are cancelled/failed, update end time, and we are done
      if (taskChild.isTerminal())
      {
        taskChild.setEndTime(Instant.now());
        taskChild = dao.updateTransferTaskChild(taskChild);
        return taskChild;
      }
      TransferTaskChild updatedChildTask = dao.getChildTaskByUUID(taskChild.getUuid());
      updatedChildTask.setStatus(TransferTaskStatus.COMPLETED);
      updatedChildTask.setEndTime(Instant.now());
      updatedChildTask = dao.updateTransferTaskChild(updatedChildTask);
      dao.updateTransferTaskParentBytesTransferred(updatedChildTask.getParentTaskId(), updatedChildTask.getBytesTransferred());
      return updatedChildTask;
    }
    catch (DAOException ex)
    {
      String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR1", taskChild.getTenantId(), taskChild.getUsername(),
                                   "stepThree", taskChild.getId(), taskChild.getUuid(), ex.getMessage());
      log.error(msg, ex);
      throw new ServiceException(msg, ex);
    }
  }

  /**
   * In this step, we check to see if there are any unfinished children for either the top level TransferTask
   *   or the TransferTaskParent.
   * If all children that belong to a parent are COMPLETED, then we can mark the parent as COMPLETED.
   * Similarly, for the top TransferTask, if ALL children have completed, the entire transfer is done.
   *
   * @param taskChild TransferTaskChild instance
   * @return updated child
   * @throws ServiceException If the updates to the records in the DB failed
   */
  private TransferTaskChild stepFour(@NotNull TransferTaskChild taskChild) throws ServiceException
  {
    try
    {
      checkForComplete(taskChild.getTaskId(), taskChild.getParentTaskId());
    }
    catch (DAOException ex)
    {
      String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR1", taskChild.getTenantId(), taskChild.getUsername(),
                                   "stepFour", taskChild.getId(), taskChild.getUuid(), ex.getMessage());
      log.error(msg, ex);
      throw new ServiceException(msg, ex);
    }
    return taskChild;
  }

  /**
   *
   * @param taskChild child task
   * @return Updated child task
   */
  private TransferTaskChild stepFive(@NotNull TransferTaskChild taskChild)
  {
    log.info("***** DOING stepFive NO-OP **** {}", taskChild);
    return taskChild;
  }

  /**
   * Error handler for any of the steps.
   * Since FAILED_OPT is now supported we also need to check here if
   *       top task / parent task are done and update status (as is done in stepFour).
   * @param msg Message from rabbitmq
   * @param cause   Exception that was thrown
   * @param child   the Child task that failed
   * @return Mono with the updated TransferTaskChild
   */
  private Mono<TransferTaskChild> doErrorStepOne(AcknowledgableDelivery msg, Throwable cause, TransferTaskChild child)
  {
    msg.nack(false);
    log.error(LibUtils.getMsg("FILES_TXFR_SVC_ERR10", child.toString()));

    // First update child task, mark FAILED_OPT or FAILED and set error message
    if (child.isOptional())
    {
      child.setStatus(TransferTaskStatus.FAILED_OPT);
    }
    else
    {
      child.setStatus(TransferTaskStatus.FAILED);
    }
    child.setErrorMessage(cause.getMessage());
    child.setEndTime(Instant.now());
    try
    {
      child = dao.updateTransferTaskChild(child);
      // In theory should never happen, it means that the child with that ID was not in the database.
      if (child == null) return Mono.empty();

      // If child is optional we need to check to see if top task and/or parent status should be updated
      // else child is required so update top level task and parent task to FAILED / FAILED_OPT
      if (child.isOptional())
      {
        checkForComplete(child.getTaskId(), child.getParentTaskId());
      }
      else
      {
        TransferTaskParent parent = dao.getTransferTaskParentById(child.getParentTaskId());
        // This also should not happen, it means that the parent with that ID was not in the database.
        if (parent == null) return Mono.empty();
        // Mark FAILED_OPT or FAILED and set error message
        // NOTE: We can have a child which is required but the parent is optional
        if (parent.isOptional())
        {
          parent.setStatus(TransferTaskStatus.FAILED_OPT);
        }
        else
        {
          parent.setStatus(TransferTaskStatus.FAILED);
        }
        parent.setEndTime(Instant.now());
        parent.setErrorMessage(cause.getMessage());
        log.error(LibUtils.getMsg("FILES_TXFR_SVC_ERR14", parent.getId(), parent.getUuid(), child.getId(), child.getUuid(), parent.getStatus()));
        dao.updateTransferTaskParent(parent);
        // If parent is required update top level task to FAILED and set error message
        if (!parent.isOptional())
        {
          TransferTask topTask = dao.getTransferTaskByID(child.getTaskId());
          // Again, should not happen, it means that the top task was not in the database.
          if (topTask == null) return Mono.empty();
          topTask.setStatus(TransferTaskStatus.FAILED);
          topTask.setErrorMessage(cause.getMessage());
          topTask.setEndTime(Instant.now());
          log.error(LibUtils.getMsg("FILES_TXFR_SVC_ERR13", topTask.getId(), topTask.getUuid(), parent.getId(), parent.getUuid(), child.getId(), child.getUuid()));
          dao.updateTransferTask(topTask);
        }
      }
    }
    catch (DAOException ex)
    {
      log.error(LibUtils.getMsg("FILES_TXFR_SVC_ERR1", child.getTenantId(), child.getUsername(),
              "doChildErrorStepOne", child.getId(), child.getUuid(), ex.getMessage()), ex);
    }

    return Mono.just(child);
  }

  // ======================================================================================
  // ========= Other private methods
  // ======================================================================================

  /**
   * Perform the transfer specified in the child task
   *
   * @param taskChild
   * @return
   * @throws ServiceException
   * @throws IOException
   */
  private TransferTaskChild doTransfer(TransferTaskChild taskChild) throws ServiceException, IOException
  {
    //We are going to run the meat of the transfer, step2 in a separate Future which we can cancel.
    //This just sets up the future, we first subscribe to the control messages and then start the future
    //which is a blocking call.
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    Future<TransferTaskChild> future = executorService.submit(new Callable<TransferTaskChild>()
    {
      @Override
      public TransferTaskChild call() throws IOException, ServiceException
      {
        return stepTwo(taskChild);
      }
    });

    //Listen for control messages and filter on the top taskID
    transfersService.streamControlMessages()
            .filter((controlAction)-> controlAction.getTaskId() == taskChild.getTaskId())
            .subscribe( (message)-> {
              future.cancel(true);
            }, (err)->{
              log.error(err.getMessage(), err);
            });
    try
    {
      // Blocking call, but the subscription above will still listen
      return future.get();
    }
    catch (ExecutionException ex)
    {
      String msg = ex.getCause().getMessage();
      log.error(msg, ex);
      if (ex.getCause() instanceof IOException)
      {
        throw new IOException(msg, ex.getCause());
      }
      else if (ex.getCause() instanceof ServiceException)
      {
        throw new ServiceException(msg, ex.getCause());
      }
      else
      {
        throw new RuntimeException(msg, ex);
      }
    }
    catch (CancellationException ex)
    {
      return cancelTransferChild(taskChild);
    }
    catch (Exception ex)
    {
      // Returning a null here tells the Flux to stop consuming downstream of step2
      return null;
    }
    finally
    {
      executorService.shutdown();
    }
  }

  /**
   * This method is called during the actual transfer of bytes. As the stream is read, events on
   * the number of bytes transferred are passed here to be written to the datastore.
   * @param bytesSent total bytes sent in latest update
   * @param taskChild The transfer task that is being worked on currently
   * @return Mono with number of bytes sent
   */
  private Mono<Long> updateProgress(Long bytesSent, TransferTaskChild taskChild)
  {
    // Be careful here if any other updates need to be done, this method (probably) runs in a different
    // thread than the main thread. It is possible for the TransferTaskChild passed in above to have been updated
    // on a different thread.
    try
    {
      dao.updateTransferTaskChildBytesTransferred(taskChild, bytesSent);
      return Mono.just(bytesSent);
    }
    catch (DAOException ex)
    {
      log.error(LibUtils.getMsg("FILES_TXFR_SVC_ERR1", taskChild.getTenantId(), taskChild.getUsername(),
              "updateProgress", taskChild.getId(), taskChild.getUuid(), ex.getMessage()));
      return Mono.empty();
    }
  }

  private TransferTaskChild cancelTransferChild(TransferTaskChild taskChild)
  {
    log.info("CANCELLING TRANSFER CHILD");
    try
    {
      taskChild.setStatus(TransferTaskStatus.CANCELLED);
      taskChild.setEndTime(Instant.now());
      taskChild = dao.updateTransferTaskChild(taskChild);
      return taskChild;
    }
    catch (DAOException ex)
    {
      log.error("CANCEL", ex);
      return null;
    }
  }

  /**
   * Helper method to extract the top level taskId which we group on for all the children tasks.
   *
   * @param message Message from rabbitmq
   * @return Id of the top level task.
   */
  private int childTaskGrouper(AcknowledgableDelivery message) throws IOException
  {
    int taskId = mapper.readValue(message.getBody(), TransferTaskChild.class).getTaskId();
    return taskId % 255;
  }

  private Mono<TransferTaskChild> deserializeChildMessage(AcknowledgableDelivery message)
  {
    try
    {
      TransferTaskChild child = mapper.readValue(message.getBody(), TransferTaskChild.class);
      return Mono.just(child);
    }
    catch (IOException ex)
    {
      // DO NOT requeue the message if it fails here!
      message.nack(false);
      return Mono.empty();
    }
  }

  /**
   * Check to see if the ParentTask and/or the top level TransferTask
   *   should be marked as finished. If yes then update status.
   *
   * @param topTaskId ID of top level TransferTask
   * @param parentTaskId ID of parent task associated with the child task
   */
  private void checkForComplete(int topTaskId, int parentTaskId) throws DAOException
  {
    TransferTask topTask = dao.getTransferTaskByID(topTaskId);
    TransferTaskParent parentTask = dao.getTransferTaskParentById(parentTaskId);
    // Check to see if all children of a parent task are complete. If so, update the parent task.
    if (!parentTask.getStatus().equals(TransferTaskStatus.COMPLETED))
    {
      long incompleteCount = dao.getIncompleteChildrenCountForParent(parentTaskId);
      if (incompleteCount == 0)
      {
        parentTask.setStatus(TransferTaskStatus.COMPLETED);
        parentTask.setEndTime(Instant.now());
        log.trace(LibUtils.getMsg("FILES_TXFR_PARENT_TASK_COMPLETE", topTaskId, topTask.getUuid(), parentTaskId, parentTask.getUuid()));
        dao.updateTransferTaskParent(parentTask);
      }
    }
    // Check to see if all the children of a top task are complete. If so, update the top task.
    if (!topTask.getStatus().equals(TransferTaskStatus.COMPLETED))
    {
      long incompleteParentCount = dao.getIncompleteParentCount(topTaskId);
      long incompleteChildCount = dao.getIncompleteChildrenCount(topTaskId);
      if (incompleteChildCount == 0 && incompleteParentCount == 0)
      {
        topTask.setStatus(TransferTaskStatus.COMPLETED);
        topTask.setEndTime(Instant.now());
        log.trace(LibUtils.getMsg("FILES_TXFR_TASK_COMPLETE2", topTaskId, topTask.getUuid()));
        dao.updateTransferTask(topTask);
      }
    }
  }
}
