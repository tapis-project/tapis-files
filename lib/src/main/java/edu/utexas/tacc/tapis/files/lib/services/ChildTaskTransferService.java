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
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskParent;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import edu.utexas.tacc.tapis.files.lib.models.TransferURI;
import edu.utexas.tacc.tapis.files.lib.transfers.ObservableInputStream;
import edu.utexas.tacc.tapis.files.lib.utils.Utils;
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
public class ChildTaskTransferService {

    private static final int MAX_RETRIES = 5;
    private final TransfersService transfersService;
    private final FileTransfersDAO dao;
    private static final ObjectMapper mapper = TapisObjectMapper.getMapper();
    private final RemoteDataClientFactory remoteDataClientFactory;
    private final SystemsCache systemsCache;
    private static final Logger log = LoggerFactory.getLogger(ChildTaskTransferService.class);

    @Inject
    public ChildTaskTransferService(TransfersService transfersService, FileTransfersDAO dao,
                                    RemoteDataClientFactory remoteDataClientFactory,
                                    SystemsCache systemsCache) {
        this.transfersService = transfersService;
        this.dao = dao;
        this.systemsCache = systemsCache;
        this.remoteDataClientFactory = remoteDataClientFactory;
    }

    /**
     * Helper method to extract the top level taskId which we group on for all the children tasks.
     *
     * @param message Message from rabbitmq
     * @return Id of the top level task.
     */
    private int childTaskGrouper(AcknowledgableDelivery message) throws IOException {
        int taskId = mapper.readValue(message.getBody(), TransferTaskChild.class).getTaskId();
        return taskId % 255;
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
     * This is the main processing workflow. Starting with the raw message stream created by streamChildMessages(),
     * we walk through the transfer process. A Flux<TransferTaskChild> is returned and can be subscribed to later
     * for further processing / notifications / logging
     *
     * @return a Flux of TransferTaskChild
     */
    public Flux<TransferTaskChild> runPipeline() {
        // Deserialize the message so that we can pass that into the reactor chain.
        return transfersService.streamChildMessages()
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
                            .flatMap(t2 -> Mono.fromCallable(() -> doTransfer(t2))
                                .retryWhen(
                                    Retry.backoff( 10, Duration.ofMillis(100))
                                        .maxBackoff(Duration.ofSeconds(120))
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
        log.info("***** DOING chevronOne ****");
        try {
            // Make sure it hasn't been cancelled already
            taskChild = dao.getTransferTaskChild(taskChild.getUuid());
            if (taskChild.isTerminal()) return taskChild;

            taskChild.setStatus(TransferTaskStatus.IN_PROGRESS);
            taskChild.setStartTime(Instant.now());
            dao.updateTransferTaskChild(taskChild);

            TransferTaskParent parentTask = dao.getTransferTaskParentById(taskChild.getParentTaskId());
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
        log.info("CANCELLING TRANSFER CHILD");
        try {
            taskChild.setStatus(TransferTaskStatus.CANCELLED);
            taskChild.setEndTime(Instant.now());
            taskChild = dao.updateTransferTaskChild(taskChild);
            return taskChild;
        } catch (DAOException ex) {
            log.error("CANCEL", ex);
            return null;
        }
    }

    public TransferTaskChild doTransfer(TransferTaskChild taskChild) throws ServiceException, IOException {

        //We are going to run the meat of the transfer, chevron2 in a separate Future which we can cancel.
        //This just sets up the future, we first subscribe to the control messages and then start the future
        //which is a blocking call.
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<TransferTaskChild> future = executorService.submit(new Callable<TransferTaskChild>() {
            @Override
            public TransferTaskChild call() throws IOException, ServiceException {
                return chevronTwo(taskChild);
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
            // Returning a null here tells the Flux to stop consuming downstream of chevron2
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

        //If we are cancelled/failed we can skip the transfer
        if (taskChild.isTerminal()) return taskChild;

        TapisSystem sourceSystem;
        TapisSystem destSystem;
        IRemoteDataClient sourceClient;
        IRemoteDataClient destClient;
        log.info("***** DOING chevronTwo **** {}", taskChild);

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


        // If we have a directory to create, just do that and return the task child
        if (taskChild.isDir()) {
            destClient.mkdir(destURL.getPath());
            return taskChild;
        }

        //Step 4: Stream the file contents to dest. While the InputStream is open,
        // we put a tap on it and send events that get grouped into 100 ms intervals. Progress
        // on the child tasks are updated during the reading of the source input stream.
        try (InputStream sourceStream = sourceClient.getStream(sourcePath);
             ObservableInputStream observableInputStream = new ObservableInputStream(sourceStream)
        ) {
            // Observe the progress event stream, just get the last event from
            // the past 1 second.
            final TransferTaskChild finalTaskChild = taskChild;
            observableInputStream.getEventStream()
                .window(Duration.ofMillis(100))
                .flatMap(window -> window.takeLast(1))
                .flatMap((progress) -> this.updateProgress(progress, finalTaskChild))
                .subscribe();
            destClient.insert(destURL.getPath(), observableInputStream);
        }




        //The ChildTransferTask gets updated in another thread so we look it up again here
        //before passing it on
        try {
            taskChild = dao.getTransferTaskChild(taskChild.getUuid());
            return taskChild;
        } catch (DAOException ex) {
            return null;
        }
    }


    /**
     * This method is called during the actual transfer of bytes. As the stream is read, events on
     * the number of bytes transferred are passed here to be written to the datastore.
     * @param bytesSent total bytes sent in latest update
     * @param taskChild The transfer task that is being worked on currently
     * @return Mono with number of bytes sent
     */
    private Mono<Long> updateProgress(Long bytesSent, TransferTaskChild taskChild) {
        // Be careful here if any other updates need to be done, this method (probably) runs in a different
        // thread than the main thread. It is possible for the TransferTaskChild passed in above to have been updated
        // on a different thread.
        try {
            dao.updateTransferTaskChildBytesTransferred(taskChild, bytesSent);
            return Mono.just(bytesSent);
        } catch (DAOException ex) {
            log.error(Utils.getMsg("FILES_TXFR_SVC_ERR1", taskChild.getTenantId(), taskChild.getUsername(),
                "updateProgress", taskChild.getId(), taskChild.getUuid(), ex.getMessage()));
            return Mono.empty();
        }
    }

    /**
     * Book keeping and cleanup. Mark the child as COMPLETE. Update the parent task with the bytes sent
     *
     * @param taskChild The child transfer task
     * @return updated TransferTaskChild
     */
    private TransferTaskChild chevronThree(@NotNull TransferTaskChild taskChild) throws ServiceException {
        // If it cancelled/failed somehow, just push it through unchanged.
        log.info("DOING chevron3 {}", taskChild);
        if (taskChild.isTerminal()) return taskChild;
        try {
            TransferTaskChild updated = dao.getChildTaskByUUID(taskChild.getUuid());
            updated.setStatus(TransferTaskStatus.COMPLETED);
            updated.setEndTime(Instant.now());
            updated = dao.updateTransferTaskChild(updated);
            dao.updateTransferTaskParentBytesTransferred(updated.getParentTaskId(), updated.getBytesTransferred());
            TransferTaskParent parent = dao.getTransferTaskParentById(updated.getParentTaskId());
            return updated;
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



}
