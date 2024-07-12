package edu.utexas.tacc.tapis.files.lib.services;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.inject.Inject;
import javax.ws.rs.NotFoundException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import edu.utexas.tacc.tapis.files.lib.clients.GlobusDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.HTTPClient;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.TransferControlAction;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskParent;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import edu.utexas.tacc.tapis.files.lib.models.TransferURI;
import edu.utexas.tacc.tapis.files.lib.rabbit.RabbitMQConnection;
import edu.utexas.tacc.tapis.files.lib.transfers.TransfersApp;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.globusproxy.client.gen.model.GlobusTransferTask;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCacheNoAuth;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.json.TapisObjectMapper;

/*
 * Transfers service methods providing functionality for TransfersApp (a worker).
 *
 * When this class is constructed, a connection is made to RabbitMQ.  When the startListeners
 * method is called, a number of channels are created, and consumers are setup.  The end result
 * is a pool of listeners that can call handleMessage (each on it's own thread).  Threads are
 * handled by rabbitMQ via an Executor service passed in when the connection is created.
 *
 * For each message that comes in, the child task service will look at the child task, and
 * copy the file described.  During the copy, a java Future is created for the transfer.  Once this
 * is setup, a new temporary cancel queue is created and a handler is setup such that if a message
 * comes in on that queue, the future can be canceled (cancelling the child transfer).  Then the
 * main thread waits for the future to complete.  Once complete, if it was successful, the
 * child task is updated.  If it was canceled, the cancel logic is applied.
 */
@Service
public class ChildTaskTransferService {
    // this ends up being the maximum number of un-acked items.  This include all items in progress as well as items
    // in the queue.  So for example if there are 5 threads and this is set to 10, we will have 5 items in progress and
    // 5 items in the queue
    private static final int QOS = 2;
    private static final int MAX_CONSUMERS = RuntimeSettings.get().getChildThreadPoolSize();
    private static String CHILD_QUEUE = "tapis.files.transfers.child";
    private static final int maxRetries = 3;
    private final TransfersService transfersService;
    private final FileTransfersDAO dao;
    private static final ObjectMapper mapper = TapisObjectMapper.getMapper();
    private final RemoteDataClientFactory remoteDataClientFactory;
    private final FileShareService shareService;
    private final FilePermsService permsService;
    private final SystemsCache systemsCache;
    private final SystemsCacheNoAuth systemsCacheNoAuth;
    private final FileUtilsService fileUtilsService;
    private static final Logger log = LoggerFactory.getLogger(ChildTaskTransferService.class);
    private Connection connection;
    private List<Channel> channels = new ArrayList<Channel>();
    private ExecutorService connectionThreadPool = null;
    private ScheduledExecutorService channelMonitorService = Executors.newSingleThreadScheduledExecutor();

    /* *********************************************************************** */
    /*            Constructors                                                 */
    /* *********************************************************************** */

    /*
     * Constructor for service.
     * Note that this is never invoked explicitly. Arguments of constructor are initialized via Dependency Injection.
     */
    @Inject
    public ChildTaskTransferService(TransfersService transfersService, FileTransfersDAO dao,
                                    FileUtilsService fileUtilsService,
                                    RemoteDataClientFactory remoteDataClientFactory,
                                    FileShareService shareService, FilePermsService permsService,
                                    SystemsCache systemsCache, SystemsCacheNoAuth systemsCacheNoAuth) throws Exception {
        this.transfersService = transfersService;
        this.dao = dao;
        this.shareService = shareService;
        this.permsService = permsService;
        this.systemsCache = systemsCache;
        this.systemsCacheNoAuth = systemsCacheNoAuth;
        this.remoteDataClientFactory = remoteDataClientFactory;
        this.fileUtilsService = fileUtilsService;

        connectionThreadPool = Executors.newFixedThreadPool(MAX_CONSUMERS);
        connection = RabbitMQConnection.getInstance().newConnection(connectionThreadPool);
    }

    /* *********************************************************************** */
    /*                      Public Methods                                     */
    /* *********************************************************************** */

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
        ChildTaskTransferService service = this;
        for (int i = 0; i < channelsToOpen; i++) {
            Channel channel = connection.createChannel();
            channel.basicQos(QOS);

            TransfersService.declareRabbitMQObjects(connection);

            channel.basicConsume(CHILD_QUEUE, false, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
                    try {
                        service.handleDelivery(channel, consumerTag, envelope, properties, body);
                    } catch (Throwable th) {
                        String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR_CONSUME_MESSAGE", consumerTag);
                        log.error(msg, th);
                    }
                }
            });
            channels.add(channel);
        }
    }

    public void handleDelivery(Channel channel, String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
        TransferTaskChild taskChild = null;

        try {
            taskChild = getChildTaskFromMessageBody(body);
            if (taskChild == null) {
                // log the error, and continue - this really shouldn't happen since we wrote the message ourselves, but being defensive.
                String msg = LibUtils.getMsg("FILES_TXFR_UNABLE_TO_PARSE_MESSAGE");
                log.error(msg);
                throw new RuntimeException(msg);
            }

            handleMessage(channel, envelope, taskChild);
        } catch (Exception ex) {
            try {
                doErrorStepOne(ex, taskChild);
                channel.basicNack(envelope.getDeliveryTag(), false, false);
            } catch (IOException e) {
                // in this case we either couldn't do our error processing or our nack
                // all we can do is log this if we get here, and throw and exception to get out.  This is due to something
                // like a db error or a rabbitmq error.
                String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR1", taskChild.getTenantId(), taskChild.getUsername(),
                        "handleDelivery", taskChild.getId(), taskChild.getTag(), taskChild.getUuid(), ex.getMessage());
                log.error(msg, e);
                throw new RuntimeException(msg, e);
            }

            // in this case we WERE able to do our error processing and our nack, but we still need to log the exception
            // all we can do is log this if we get here, and throw and exception to get out.  This is due to something
            // like a db error or a rabbitmq error.
            String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR1", taskChild.getTenantId(), taskChild.getUsername(),
                    "handleDelivery", taskChild.getId(), taskChild.getTag(), taskChild.getUuid(), ex.getMessage());
            log.error(msg, ex);
            throw new RuntimeException(msg, ex);
        }
    }

    public void handleMessage(Channel channel, Envelope envelope, TransferTaskChild taskChild) throws IOException {

        int retry = 0;
        boolean preTransferUpdateComplete = false;
        boolean transferComplete = false;
        boolean postTransferUpdateComplete = false;
        boolean parentCheckComplete = false;
        Exception lastException = null;


        // save some info in case we have an error
        String tenantId = taskChild.getTenantId();
        String user = taskChild.getUsername();
        int id = taskChild.getId();
        String tag = taskChild.getTag();
        UUID uuid = taskChild.getUuid();

        while (retry < maxRetries) {
            try {
                if(!preTransferUpdateComplete) {
                    taskChild = updateStatusBeforeTransfer(taskChild);
                    if (taskChild == null) {
                        // if updateStatusBeforeTransfer fails, it throws an exception.  We shouldn't get here.  Just being defensive
                        String msg = LibUtils.getMsg("Internal Error.  taskChild is null after updateStatusBeforeTransfer");
                        throw new IOException(msg);
                    } else {
                        preTransferUpdateComplete = true;
                    }
                }

                if(!transferComplete) {
                    taskChild = doTransfer(taskChild);
                    if (taskChild == null) {
                        // if doTransfer fails, it throws an exception.  We shouldn't get here.  Just being defensive
                        String msg = LibUtils.getMsg("Internal Error.  taskChild is null after doTransfer");
                        throw new IOException(msg);
                    } else {
                        transferComplete = true;
                    }
                }

                if(!postTransferUpdateComplete) {
                    taskChild = updateStatusAfterTransfer(taskChild);
                    if (taskChild == null) {
                        // if updateStatusAfterTransfer fails, it throws an exception.  We shouldn't get here.  Just being defensive
                        String msg = LibUtils.getMsg("Internal Error.  taskChild is null after updateStatusAfterTransfer");
                        throw new IOException(msg);
                    } else {
                        postTransferUpdateComplete = true;
                    }
                }

                if(!parentCheckComplete) {
                    taskChild = checkForParentCompletion(taskChild);
                    if (taskChild == null) {
                        // if checkForParentCompletion fails, it throws an exception.  We shouldn't get here.  Just being defensive
                        String msg = LibUtils.getMsg("Internal Error.  taskChild is null after checkForParentComplete");
                        throw new IOException(msg);
                    }
                }

                channel.basicAck(envelope.getDeliveryTag(), false);
                return;
            } catch (Exception e) {
                String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR1", tenantId, user,
                        "handleMessage", id, tag, uuid, e.getMessage());
                log.error(msg, e);
                lastException = e;
            }
            retry++;
        }

        doErrorStepOne(lastException, taskChild);
        // out of retries, so give up on this one.
        channel.basicNack(envelope.getDeliveryTag(), false, false);
    }

    public void nackMessage(Channel channel, Envelope envelope, boolean requeue) throws IOException {
        channel.basicNack(envelope.getDeliveryTag(), false, requeue);
    }

    private TransferTaskChild getChildTaskFromMessageBody(byte[] messageBody) throws JsonProcessingException {
        String jsonMessage = new String(messageBody, StandardCharsets.UTF_8);
        return TapisObjectMapper.getMapper().readValue(jsonMessage, TransferTaskChild.class);
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
    private TransferTaskChild updateStatusBeforeTransfer(@NotNull TransferTaskChild taskChild) throws ServiceException {
        String stepLabel = "One";
        log.info(LibUtils.getMsg("FILES_TXFR_CHILD_TASK", stepLabel, taskChild));
        // Update parent task and then child task
        try {
            taskChild = dao.getTransferTaskChild(taskChild.getUuid());
            TransferTask topTask = dao.getTransferTaskByID(taskChild.getTaskId());
            TransferTaskParent parentTask = dao.getTransferTaskParentById(taskChild.getParentTaskId());
            // UUIDs for topTask, parentTask, childTask
            String topTaskUUID = topTask.getUuid().toString();
            String parentTaskUUID = parentTask.getUuid().toString();
            String childTaskUUID = taskChild.getUuid().toString();
            log.debug(LibUtils.getMsg("FILES_TXFR_CHILD_STEP", stepLabel, topTaskUUID, parentTaskUUID, childTaskUUID, taskChild.getTag()));
            log.debug(LibUtils.getMsg("FILES_TXFR_PARENT_TASK", stepLabel, parentTask));
            // If the parent task not in final state and not yet set to IN_PROGRESS do it here.
            if (!parentTask.isTerminal() && !parentTask.getStatus().equals(TransferTaskStatus.IN_PROGRESS)) {
                parentTask.setStatus(TransferTaskStatus.IN_PROGRESS);
                if (parentTask.getStartTime() == null) parentTask.setStartTime(Instant.now());
                dao.updateTransferTaskParent(parentTask);
            }

            // If cancelled or failed set the end time, and we are done
            if (taskChild.isTerminal()) {
                taskChild.setEndTime(Instant.now());
                taskChild = dao.updateTransferTaskChild(taskChild);
                return taskChild;
            }

            // Ready to start. Update child task status and start time.
            taskChild.setStatus(TransferTaskStatus.IN_PROGRESS);
            taskChild.setStartTime(Instant.now());
            taskChild = dao.updateTransferTaskChild(taskChild);

            return taskChild;
        } catch (DAOException ex) {
            String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR1", taskChild.getTenantId(), taskChild.getUsername(),
                    "ChildStepOne", taskChild.getId(), taskChild.getTag(), taskChild.getUuid(), ex.getMessage());
            log.error(msg, ex);
            throw new ServiceException(msg, ex);
        }
    }

    /**
     * Perform the transfer, this is the meat of the operation.
     *
     * @param taskChild the incoming child task
     * @return update child task
     * @throws ServiceException If the DAO updates failed or a transfer failed in flight
     */
    private TransferTaskChild processTransfer(TransferTaskChild taskChild) throws ServiceException, NotFoundException, IOException {
        String opName = "childTaskTxfr";
        String stepLabel = "Two";
        log.info(LibUtils.getMsg("FILES_TXFR_CHILD_TASK", stepLabel, taskChild));
        boolean srcIsLinux = false, dstIsLinux = false; // Used for properly handling update of exec perm

        TapisSystem sourceSystem = null;
        TapisSystem destSystem = null;
        IRemoteDataClient sourceClient;
        IRemoteDataClient destClient;
        if (taskChild.getSourceURI().equals(taskChild.getDestinationURI())) {
            log.warn("***** Source and Destination URI's are identical - skipping transfer, and marking complete **** {}", taskChild);
            try {
                taskChild.setEndTime(Instant.now());
                taskChild.setStatus(TransferTaskStatus.COMPLETED);
                taskChild.setBytesTransferred(taskChild.getTotalBytes());
                return dao.updateTransferTaskChild(taskChild);
            } catch (DAOException ex) {
                String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR1", taskChild.getTenantId(), taskChild.getUsername(),
                        "ChildStepTwoA", taskChild.getId(), taskChild.getTag(), taskChild.getUuid(), ex.getMessage());
                log.error(msg, ex);
                throw new ServiceException(msg, ex);
            }
        }

        TransferTaskParent parentTask;
        try {
            // Get the parent task. We will need it for shared ctx grantors.
            parentTask = dao.getTransferTaskParentById(taskChild.getParentTaskId());
            if (TransferTaskStatus.CANCELLED.equals(parentTask.getStatus()) || TransferTaskStatus.FAILED.equals(parentTask.getStatus())) {
                if (!taskChild.isTerminal()) {
                    taskChild.setStatus(parentTask.getStatus());
                }
            }

            // If cancelled or failed set the end time, and we are done
            if (taskChild.isTerminal()) {
                taskChild.setEndTime(Instant.now());
                taskChild = dao.updateTransferTaskChild(taskChild);
                return taskChild;
            }

            // Update task in DB to IN_PROGRESS and increment the retries on this particular task
            taskChild.setStatus(TransferTaskStatus.IN_PROGRESS);
            taskChild.setRetries(taskChild.getRetries() + 1);
            taskChild = dao.updateTransferTaskChild(taskChild);

            // For some reason taskChild does not have the tag set at this point.
            taskChild.setTag(parentTask.getTag());
        } catch (DAOException ex) {
            String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR1", taskChild.getTenantId(), taskChild.getUsername(),
                    "ChildStepTwoA", taskChild.getId(), taskChild.getTag(), taskChild.getUuid(), ex.getMessage());
            log.error(msg, ex);
            throw new ServiceException(msg, ex);
        }

        TransferURI destURL = taskChild.getDestinationURI();
        TransferURI sourceURL = taskChild.getSourceURI();
        String impersonationIdNull = null; // to help with debugging
        String sourcePath;
        String destPath = destURL.getPath();

        // Simulate a ResourceRequestUser since we will need to make some calls that require it
        // Obo tenant and user come from task, jwt tenant and user are files@<site admin tenant>
        String oboUser = taskChild.getUsername();
        String oboTenant = taskChild.getTenantId();
        String jwtUser = TapisConstants.SERVICE_NAME_FILES;
        String jwtTenant = TransfersApp.getSiteAdminTenantId();
        ResourceRequestUser rUser =
                new ResourceRequestUser(new AuthenticatedUser(jwtUser, jwtTenant, TapisThreadContext.AccountType.service.name(),
                        null, oboUser, oboTenant, null, null, null));

        // Initialize source path and client
        if (taskChild.getSourceURI().toString().startsWith("https://") || taskChild.getSourceURI().toString().startsWith("http://")) {
            // Source is HTTP/S
            // Pass in the full URLs as strings
            sourcePath = sourceURL.toString();
            sourceClient = new HTTPClient(taskChild.getTenantId(), taskChild.getUsername(), sourceURL.toString(), destURL.toString());
        } else {
            // Source is not HTTP/S. At this point both source and destination should be Tapis URLs
            sourcePath = sourceURL.getPath();
            // Use a LibUtils method to properly take into account ownership, sharing and fine-grained permissions.
            sourceSystem = LibUtils.getResolvedSysWithAuthCheck(rUser, shareService, systemsCache, systemsCacheNoAuth,
                    permsService, opName, sourceURL.getSystemId(), sourcePath, FileInfo.Permission.READ, impersonationIdNull,
                    parentTask.getSrcSharedCtxGrantor());
            // If src system is not enabled throw an exception
            if (sourceSystem.getEnabled() == null || !sourceSystem.getEnabled()) {
                String msg = LibUtils.getMsg("FILES_TXFR_SYS_NOTENABLED", taskChild.getTenantId(),
                        taskChild.getUsername(), taskChild.getId(), taskChild.getUuid(), sourceSystem.getId(), taskChild.getTag());
                throw new ServiceException(msg);
            }

            // Used for properly handling update of exec perm
            srcIsLinux = SystemTypeEnum.LINUX.equals(sourceSystem.getSystemType());

            sourceClient = remoteDataClientFactory.getRemoteDataClient(taskChild.getTenantId(), taskChild.getUsername(), sourceSystem);
        }

        // Initialize destination client
        try {
            // Use a LibUtils method to properly take into account ownership, sharing and fine-grained permissions.
            destSystem = LibUtils.getResolvedSysWithAuthCheck(rUser, shareService, systemsCache, systemsCacheNoAuth,
                    permsService, opName, destURL.getSystemId(), destPath, FileInfo.Permission.MODIFY, impersonationIdNull,
                    parentTask.getSrcSharedCtxGrantor());
            // If dst system is not enabled throw an exception
            if (destSystem.getEnabled() == null || !destSystem.getEnabled()) {
                String msg = LibUtils.getMsg("FILES_TXFR_SYS_NOTENABLED", taskChild.getTenantId(), taskChild.getUsername(),
                        taskChild.getId(), taskChild.getUuid(), destSystem.getId(), taskChild.getTag());
                log.error(msg);
                throw new ServiceException(msg);
            }

            // Used for properly handling update of exec perm
            dstIsLinux = SystemTypeEnum.LINUX.equals(destSystem.getSystemType());

            destClient = remoteDataClientFactory.getRemoteDataClient(taskChild.getTenantId(), taskChild.getUsername(), destSystem);
        } catch (IOException | ServiceException ex) {
            String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR1", taskChild.getTenantId(), taskChild.getUsername(),
                    "ChildStepTwoB", taskChild.getId(), taskChild.getTag(), taskChild.getUuid(), ex.getMessage());
            log.error(msg, ex);
            throw new ServiceException(msg, ex);
        }

        // Determine if it is a Globus transfer. Consider it a Globus transfer if either source or destination
        //   is of type GLOBUS. Note that currently only Globus to Globus is supported.
        boolean isGlobus = ((sourceSystem != null && SystemTypeEnum.GLOBUS.equals(sourceSystem.getSystemType())) ||
                             SystemTypeEnum.GLOBUS.equals(destSystem.getSystemType()));

        // If destination is a directory and not doing a Globus transfer, then create the directory and return
        if (taskChild.isDir() && !isGlobus) {
            destClient.mkdir(destURL.getPath());
            // The ChildTransferTask may have been updated by calling thread, e.g. cancelled, so we look it up again
            // here before passing it on
            try {
                taskChild = dao.getTransferTaskChild(taskChild.getUuid());
            } catch (DAOException ex) {
                String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR1", taskChild.getTenantId(), taskChild.getUsername(),
                        "processTransfer", taskChild.getId(), taskChild.getTag(), taskChild.getUuid(), ex.getMessage());
                throw new ServiceException(msg);
            }
            return taskChild;
        }

        // Handle as Globus or non-Globus transfer
        if (isGlobus) {
            performASynchFileTransfer(taskChild, sourceClient, sourceURL, destClient, destURL);
        } else {
            performSynchFileTransfer(taskChild, sourceClient, sourceURL, destClient, destURL, sourcePath);
        }

        // If it is an executable file on a posix system going to a posix system, chmod it to be +x.
        // Note: sourceSystem will be null and srcIsLinux will be false if source is http/s.
        if (sourceSystem != null && srcIsLinux && dstIsLinux) {
            // Figure out if dest system is shared. We need to know if we should turn of perm checking.
            // First check to see if we are in a sharedCtx
            boolean isDestShared = !StringUtils.isBlank(parentTask.getDestSharedCtxGrantor());
            // Even if not in a sharedCtx the dest system may be shared publicly or directly with the user.
            if (!isDestShared) {
                boolean isSharedPublic = destSystem.getIsPublic() == null ? false : destSystem.getIsPublic();
                List<String> sharedWithUsers = destSystem.getSharedWithUsers();
                boolean isSharedDirect = (sharedWithUsers != null && sharedWithUsers.contains(oboUser));
                isDestShared = (isSharedPublic || isSharedDirect);
            }
            updateLinuxExeFile(taskChild, sourceClient, sourceURL, destClient, destURL, isDestShared);
        }

        // The ChildTransferTask may have been updated by calling thread, e.g. cancelled, so we look it up again
        // here before passing it on
        try {
            taskChild = dao.getTransferTaskChild(taskChild.getUuid());
        } catch (DAOException ex) {
            String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR1", taskChild.getTenantId(), taskChild.getUsername(),
                    "processTransfer", taskChild.getId(), taskChild.getTag(), taskChild.getUuid(), ex.getMessage());
            throw new ServiceException(msg);
        }
        return taskChild;
    }

    /**
     * Bookkeeping and cleanup. Mark the child as COMPLETE. Update the parent task with the bytes sent
     *
     * @param taskChild The child transfer task
     * @return updated TransferTaskChild
     */
    private TransferTaskChild updateStatusAfterTransfer(@NotNull TransferTaskChild taskChild) throws ServiceException {
        String stepLabel = "Three";
        log.info(LibUtils.getMsg("FILES_TXFR_CHILD_TASK", stepLabel, taskChild));
        // If it cancelled/failed somehow, just push it through unchanged.
        try {
            // If we are cancelled/failed, update end time, and we are done
            if (taskChild.isTerminal()) {
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
        } catch (DAOException ex) {
            String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR1", taskChild.getTenantId(), taskChild.getUsername(),
                    "ChildStepThree", taskChild.getId(), taskChild.getTag(), taskChild.getUuid(), ex.getMessage());
            log.error(msg, ex);
            throw new ServiceException(msg, ex);
        }
    }

    /**
     * In this step, we check to see if there are any unfinished children for either the top level TransferTask
     * or the TransferTaskParent.
     * If all children that belong to a parent are COMPLETED, then we can mark the parent as COMPLETED.
     * Similarly, for the top TransferTask, if ALL children have completed, the entire transfer is done.
     *
     * @param taskChild TransferTaskChild instance
     * @return updated child
     * @throws ServiceException If the updates to the records in the DB failed
     */
    private TransferTaskChild checkForParentCompletion(@NotNull TransferTaskChild taskChild) throws ServiceException {
        String stepLabel = "Four";
        log.info(LibUtils.getMsg("FILES_TXFR_CHILD_TASK", stepLabel, taskChild));
        try {
            checkForComplete(taskChild.getTaskId(), taskChild.getParentTaskId());
        } catch (DAOException ex) {
            String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR1", taskChild.getTenantId(), taskChild.getUsername(),
                    "ChildStepFour", taskChild.getId(), taskChild.getTag(), taskChild.getUuid(), ex.getMessage());
            log.error(msg, ex);
            throw new ServiceException(msg, ex);
        }
        return taskChild;
    }

    /**
     * Error handler for any of the steps.
     * Since FAILED_OPT is now supported we also need to check here if
     * top task / parent task are done and update status (as is done in stepFour).
     *
     * @param cause Exception that was thrown
     * @param child the Child task that failed
     * @return Mono with the updated TransferTaskChild
     */
    private TransferTaskChild doErrorStepOne(Throwable cause, TransferTaskChild child) {
        String exceptionMessage = (cause == null) ? "<NULL>" : cause.getMessage();
        log.error(LibUtils.getMsg("FILES_TXFR_SVC_ERR10", child.toString()));

        // First update child task, mark FAILED_OPT or FAILED and set error message
        if (child.isOptional()) {
            child.setStatus(TransferTaskStatus.FAILED_OPT);
        } else {
            child.setStatus(TransferTaskStatus.FAILED);
        }
        child.setErrorMessage(exceptionMessage);
        child.setEndTime(Instant.now());
        try {
            child = dao.updateTransferTaskChild(child);
            // In theory should never happen, it means that the child with that ID was not in the database.
            if (child == null) {
                return null;
            }

            // If child is optional we need to check to see if top task and/or parent status should be updated
            // else child is required so update top level task and parent task to FAILED / FAILED_OPT
            if (child.isOptional()) {
                checkForComplete(child.getTaskId(), child.getParentTaskId());
            } else {
                TransferTaskParent parent = dao.getTransferTaskParentById(child.getParentTaskId());
                // This also should not happen, it means that the parent with that ID was not in the database.
                if (parent == null) {
                    return null;
                }
                // Mark FAILED_OPT or FAILED and set error message
                // NOTE: We can have a child which is required but the parent is optional
                if (parent.isOptional()) {
                    parent.setStatus(TransferTaskStatus.FAILED_OPT);
                } else {
                    parent.setStatus(TransferTaskStatus.FAILED);
                }
                parent.setEndTime(Instant.now());
                parent.setErrorMessage(exceptionMessage);
                parent.setFinalMessage("Failed - Child doErrorStepOne");
                log.error(LibUtils.getMsg("FILES_TXFR_SVC_ERR14", parent.getId(), parent.getTag(), parent.getUuid(), child.getId(), child.getUuid(), parent.getStatus()));
                dao.updateTransferTaskParent(parent);
                // If parent is required update top level task to FAILED and set error message
                if (!parent.isOptional()) {
                    TransferTask topTask = dao.getTransferTaskByID(child.getTaskId());
                    // Again, should not happen, it means that the top task was not in the database.
                    if (topTask == null) {
                        return null;
                    }
                    topTask.setStatus(TransferTaskStatus.FAILED);
                    topTask.setErrorMessage(exceptionMessage);
                    topTask.setEndTime(Instant.now());
                    log.error(LibUtils.getMsg("FILES_TXFR_SVC_ERR13", topTask.getId(), topTask.getTag(), topTask.getUuid(), parent.getId(), parent.getUuid(), child.getId(), child.getUuid()));
                    dao.updateTransferTask(topTask);
                }
            }
        } catch (DAOException ex) {
            log.error(LibUtils.getMsg("FILES_TXFR_SVC_ERR1", child.getTenantId(), child.getUsername(),
                    "doChildErrorStepOne", child.getId(), child.getTag(), child.getUuid(), ex.getMessage()), ex);
        }

        return child;
    }

    // ======================================================================================
    // ========= Other private methods
    // ======================================================================================

    /*
     * Perform the transfer specified in the child task
     */
    private TransferTaskChild doTransfer(TransferTaskChild taskChild) throws Exception {
        //We are going to run the meat of the transfer, step2 in a separate Future which we can cancel.
        //This just sets up the future, we first subscribe to the control messages and then start the future
        //which is a blocking call.
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<TransferTaskChild> future = executorService.submit(new Callable<TransferTaskChild>() {
            @Override
            public TransferTaskChild call() throws IOException, ServiceException {
                return processTransfer(taskChild);
            }
        });

        Channel channel = connection.createChannel();
        String queueName = "control." + UUID.randomUUID();

        channel.queueDeclare(queueName, false, false, true, null);
        channel.exchangeDeclare(TransfersService.CONTROL_EXCHANGE, BuiltinExchangeType.FANOUT, true);
        channel.queueBind(queueName, TransfersService.CONTROL_EXCHANGE, "#");

        channel.basicConsume(queueName, false, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                TransferControlAction action = mapper.readValue(body, TransferControlAction.class);

                // the control exchange is a "fan out" exchange, so each queue gets ALL of the control messages.
                // This means some will not belong to us, so check to make sure it's the right one.  All messages
                // must be acknoledged though - the ones not for us will be in the correct queues also.  At some
                // future time, we should change this to be a message header excahnge so we can select the proper
                // messages - but this is what the old code did, and it works.
                try {
                    if (taskChild.getTaskId() == action.getTaskId()) {
                        future.cancel(true);
                    }
                } finally {
                    channel.basicAck(envelope.getDeliveryTag(), false);
                }
            }
        });

        try {
            // Blocking call, but the subscription above will still listen
            TransferTaskChild returnChild = future.get();
            return returnChild;
        } catch (ExecutionException ex) {
            String msg = ex.getCause().getMessage();
            log.error(msg, ex);
            if (ex.getCause() instanceof IOException) {
                throw new IOException(msg, ex.getCause());
            } else if (ex.getCause() instanceof ServiceException) {
                throw new ServiceException(msg, ex.getCause());
            } else {
                throw new RuntimeException(msg, ex);
            }
        } catch (CancellationException ex) {
            return cancelTransferChild(taskChild);
        } finally {
            try {
                channel.close();
            } catch (TimeoutException e) {
                throw new RuntimeException(e);
            }
            executorService.shutdown();
        }
    }

    /**
     * This method is called during the actual transfer of bytes. As the stream is read, events on
     * the number of bytes transferred are passed here to be written to the datastore.
     *
     * @param bytesSent total bytes sent in latest update
     * @param taskChild The transfer task that is being worked on currently
     * @return Mono with number of bytes sent
     */
    private Long updateProgress(Long bytesSent, TransferTaskChild taskChild) {
        // Be careful here if any other updates need to be done, this method (probably) runs in a different
        // thread than the main thread. It is possible for the TransferTaskChild passed in above to have been updated
        // on a different thread.
        try {
            dao.updateTransferTaskChildBytesTransferred(taskChild, bytesSent);
            return bytesSent;
        } catch (DAOException ex) {
            log.error(LibUtils.getMsg("FILES_TXFR_SVC_ERR1", taskChild.getTenantId(), taskChild.getUsername(),
                    "updateProgress", taskChild.getId(), taskChild.getTag(), taskChild.getUuid(), ex.getMessage()));
            return null;
        }
    }

    private TransferTaskChild cancelTransferChild(TransferTaskChild taskChild) throws ServiceException, IOException {
        TransferTaskChild retChild;
        log.info("CANCELLING TRANSFER CHILD");
        taskChild.setStatus(TransferTaskStatus.CANCELLED);
        taskChild.setEndTime(Instant.now());
        try {
            retChild = dao.updateTransferTaskChild(taskChild);
        } catch (DAOException ex) {
            // On DAO error still might need to attempt the globus cancel, so log error and continue.
            log.error("CANCEL", ex);
            retChild = null;
        }

        // Get the source system, so we can check if it is a globus transfer
        TransferURI srcUri = taskChild.getSourceURI();
        TapisSystem srcSys =
                systemsCacheNoAuth.getSystem(taskChild.getTenantId(), srcUri.getSystemId(), taskChild.getUsername());
        // If an external globus transfer, send a cancel request to globus-proxy
        if (!StringUtils.isBlank(taskChild.getExternalTaskId()) && srcSys != null &&
                SystemTypeEnum.GLOBUS.equals(srcSys.getSystemType())) {
            // Get the client for the source system and make sure it is the expected type.
            IRemoteDataClient srcClient =
                    remoteDataClientFactory.getRemoteDataClient(taskChild.getTenantId(), taskChild.getUsername(), srcSys);
            if (!(srcClient instanceof GlobusDataClient)) {
                throw new ServiceException(LibUtils.getMsg("FILES_TXFR_GLOBUS_WRONG_CLIENT", "Source", srcUri, taskChild.getTag()));
            }
            var gSrcClient = (GlobusDataClient) srcClient;
            // Use srcClient to call GlobusProxy to cancel the task
            gSrcClient.cancelGlobusTransferTask(taskChild.getExternalTaskId());
        }
        return retChild;
    }

    /**
     * Check to see if the ParentTask and/or the top level TransferTask
     * should be marked as finished. If yes then update status.
     *
     * @param topTaskId    ID of top level TransferTask
     * @param parentTaskId ID of parent task associated with the child task
     */
    private void checkForComplete(int topTaskId, int parentTaskId) throws DAOException {
        TransferTask topTask = dao.getTransferTaskByID(topTaskId);
        TransferTaskParent parentTask = dao.getTransferTaskParentById(parentTaskId);
        // Check to see if all children of a parent task are complete. If so, update the parent task.
        if (!parentTask.getStatus().equals(TransferTaskStatus.COMPLETED)) {
            long incompleteCount = dao.getIncompleteChildrenCountForParent(parentTaskId);
            if (incompleteCount == 0) {
                parentTask.setStatus(TransferTaskStatus.COMPLETED);
                parentTask.setEndTime(Instant.now());
                parentTask.setFinalMessage("Completed");
                log.trace(LibUtils.getMsg("FILES_TXFR_PARENT_TASK_COMPLETE", topTaskId, topTask.getUuid(), parentTaskId, parentTask.getUuid(), parentTask.getTag()));
                dao.updateTransferTaskParent(parentTask);
            }
        }
        // Check to see if all the children of a top task are complete. If so, update the top task.
        if (!topTask.getStatus().equals(TransferTaskStatus.COMPLETED)) {
            long incompleteParentCount = dao.getIncompleteParentCount(topTaskId);
            long incompleteChildCount = dao.getIncompleteChildrenCount(topTaskId);
            if (incompleteChildCount == 0 && incompleteParentCount == 0) {
                topTask.setStatus(TransferTaskStatus.COMPLETED);
                topTask.setEndTime(Instant.now());
                log.trace(LibUtils.getMsg("FILES_TXFR_TASK_COMPLETE2", topTaskId, topTask.getUuid(), topTask.getTag()));
                dao.updateTransferTask(topTask);
            }
        }
    }


    /**
     * Update perm on LINUX exe file
     *
     * @param taskChild task we are processing
     * @param srcClient Remote data client for source system
     * @param srcUri    source path as URI
     * @param dstClient Remote data client for destination system
     * @param dstUri    Destination path as URI
     */
    private void updateLinuxExeFile(TransferTaskChild taskChild,
                                    IRemoteDataClient srcClient, TransferURI srcUri,
                                    IRemoteDataClient dstClient, TransferURI dstUri,
                                    boolean isDestShared)
            throws IOException, ServiceException {

        String srcPath = srcUri.getPath();
        String dstPath = dstUri.getPath();
        FileInfo item = srcClient.getFileInfo(srcPath, true);
        if (item == null) {
            throw new NotFoundException(LibUtils.getMsg("FILES_TXFR_CHILD_PATH_NOTFOUND", taskChild.getTenantId(),
                    taskChild.getUsername(), taskChild.getId(),
                    taskChild.getUuid(), srcPath, taskChild.getTag()));
        }
        if (!item.isDir() && item.getNativePermissions().contains("x")) {
            try {
                // If in a sharedAppCtx, tell linuxOp to skip the perms check.
                // so the linuxOp will skip the perm check
                boolean recurseFalse = false;
                fileUtilsService.linuxOp(dstClient, dstPath, FileUtilsService.NativeLinuxOperation.CHMOD, "700",
                        recurseFalse, isDestShared);
            } catch (TapisException ex) {
                String msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR1", taskChild.getTenantId(), taskChild.getUsername(),
                        "chmod", taskChild.getId(), taskChild.getTag(), taskChild.getUuid(), ex.getMessage());
                log.error(msg, ex);
                throw new ServiceException(msg, ex);
            }
        }
    }

    /**
     * Perform synchronous transfer between two systems using a stream
     *
     * @param taskChild task we are processing
     * @param srcClient Remote data client for source system
     * @param srcUri    source path as URI
     * @param dstClient Remote data client for destination system
     * @param dstUri    Destination path as URI
     * @param srcPath   Source path as string. For HTTP/S this is full URL
     */
    private void performSynchFileTransfer(TransferTaskChild taskChild,
                                          IRemoteDataClient srcClient, TransferURI srcUri,
                                          IRemoteDataClient dstClient, TransferURI dstUri,
                                          String srcPath)
            throws IOException {
        String dstPath = dstUri.getPath();
        String msg = LibUtils.getMsg("FILES_TXFR_CHILD_SYNCH_BEGIN", taskChild.getTenantId(), taskChild.getUsername(),
                taskChild.getId(), taskChild.getTag(), taskChild.getUuid(),
                srcUri.getSystemId(), srcPath, dstUri.getSystemId(), dstPath);
        log.trace(msg);
        // Stream the file contents to destination. While the InputStream is open,
        // we put a tap on it and send events that get grouped into 100 ms intervals. Progress
        // on the child tasks are updated during the reading of the source input stream.
        try (InputStream sourceStream = srcClient.getStream(srcPath)) {
            // Observe the progress event stream, just get the last event from the past 1 second.
            final TransferTaskChild finalTaskChild = taskChild;
            dstClient.upload(dstPath, sourceStream);
        }
        msg = LibUtils.getMsg("FILES_TXFR_CHILD_SYNCH_END", taskChild.getTenantId(), taskChild.getUsername(),
                taskChild.getId(), taskChild.getTag(), taskChild.getUuid(),
                srcUri.getSystemId(), srcPath, dstUri.getSystemId(), dstPath);
        log.trace(msg);
    }

    /**
     * Perform asynchronous transfer between two systems for the case where Tapis is not in control of the transfer.
     * All incoming arguments must be non-null
     * Handle a GLOBUS type point to point txfr where we are not in control of the stream.
     * We initiate the transfer, get the externalTransferId, update the child task
     * with the externalTransferId and then monitor the transfer and update the progress.
     *
     * @param taskChild task we are processing
     * @param srcClient Remote data client for source system
     * @param srcUri    source path as URI
     * @param dstClient Remote data client for destination system
     * @param dstUri    Destination path as URI
     */
    private void performASynchFileTransfer(TransferTaskChild taskChild,
                                           IRemoteDataClient srcClient, TransferURI srcUri,
                                           IRemoteDataClient dstClient, TransferURI dstUri)
            throws ServiceException {
        // None of the incoming arguments should be null
        if (taskChild == null || srcClient == null || srcUri == null || dstUri == null ||
                srcClient.getSystem() == null || dstClient.getSystem() == null) {
            throw new ServiceException(LibUtils.getMsg("FILES_TXFR_ASYNCH_NULL", taskChild, srcClient, srcUri, dstClient, dstUri));
        }
        // Use some local variables for convenience and readability
        String tag = taskChild.getTag();
        TapisSystem srcSys = srcClient.getSystem();
        TapisSystem dstSys = dstClient.getSystem();
        String srcRelPath = srcUri.getPath();
        String dstRelPath = dstUri.getPath();
        String dstHost = dstSys.getHost();
        String dstRootDir = dstSys.getRootDir();
        String dstSysId = dstSys.getId();

        String msg = LibUtils.getMsg("FILES_TXFR_CHILD_ASYNCH_BEGIN", taskChild.getTenantId(), taskChild.getUsername(),
                taskChild.getId(), taskChild.getTag(), taskChild.getUuid(),
                srcUri.getSystemId(), srcRelPath, dstUri.getSystemId(), dstRelPath);
        log.trace(msg);
        // Check that both src and dst are Globus. If not throw a ServiceException
        // If srcSys is GLOBUS and dstSys is not then we do not support it OR
        //    dstSys is GLOBUS and srcSys is not then we do not support it
        if ((SystemTypeEnum.GLOBUS.equals(srcSys.getSystemType()) &&
                !SystemTypeEnum.GLOBUS.equals(dstSys.getSystemType())) ||
                (SystemTypeEnum.GLOBUS.equals(dstSys.getSystemType()) &&
                        !SystemTypeEnum.GLOBUS.equals(srcSys.getSystemType()))) {
            throw new ServiceException(LibUtils.getMsg("FILES_TXFR_GLOBUS_NOTSUPPORTED", srcUri, dstUri, tag));
        }

        // If we somehow get here both clients must be of type GlobusDataClient.
        if (!(srcClient instanceof GlobusDataClient)) {
            throw new ServiceException(LibUtils.getMsg("FILES_TXFR_GLOBUS_WRONG_CLIENT", "Source", srcUri, tag));
        }
        if (!(dstClient instanceof GlobusDataClient)) {
            throw new ServiceException(LibUtils.getMsg("FILES_TXFR_GLOBUS_WRONG_CLIENT", "Destination", dstUri, tag));
        }

        // TODO: Use a backoff policy?
        long pollIntervalSeconds = RuntimeSettings.get().getAsyncTransferPollSeconds();
        String externalTaskId = null;
        try {
            // Use srcClient to call GlobusProxy to kick off a transfer originating from the source Globus system
            var gSrcClient = ((GlobusDataClient) srcClient);
            GlobusTransferTask externalTask = gSrcClient.createFileTransferTaskFromEndpoint(srcRelPath, dstSysId, dstRootDir,
                    dstHost, dstRelPath);
            if (externalTask == null || StringUtils.isBlank(externalTask.getTaskId())) {
                throw new ServiceException(LibUtils.getMsg("FILES_TXFR_ASYNCH_NULL_TASK", externalTask));
            }
            externalTaskId = externalTask.getTaskId();
            msg = LibUtils.getMsg("FILES_TXFR_CHILD_ASYNCH_EXTERNAL", taskChild.getTenantId(), taskChild.getUsername(),
                    taskChild.getId(), taskChild.getTag(), taskChild.getUuid(), externalTask.toString());
            log.trace(msg);

            // Update child task with external task id.
            taskChild.setExternalTaskId(externalTaskId);
            taskChild = dao.updateTransferTaskChild(taskChild);
            // Monitor the status of the transfer until it is in a final state.
            // Loop forever waiting for task to finish or be cancelled.
            int iteration = 1;
            while (true) {
                // Get latest taskChild info to see if it has been cancelled
                taskChild = dao.getTransferTaskChild(taskChild.getUuid());
                // If in a terminal state, e.g. cancelled, set the end time and exit loop
                if (taskChild.isTerminal()) {
                    taskChild.setEndTime(Instant.now());
                    taskChild = dao.updateTransferTaskChild(taskChild);
                    break;
                }
                // Wait poll interval between status checks.
                log.trace(LibUtils.getMsg("FILES_TXFR_ASYNCH_POLL", taskChild.getTenantId(), taskChild.getUsername(),
                        taskChild.getId(), tag, taskChild.getUuid(), pollIntervalSeconds, iteration));
                try {
                    Thread.sleep(pollIntervalSeconds * 1000);
                } catch (InterruptedException e) {
                    log.warn(LibUtils.getMsg("FILES_TXFR_ASYNCH_INTERRUPTED", taskChild.getTenantId(), taskChild.getUsername(),
                            taskChild.getId(), taskChild.getTag(), taskChild.getUuid(), iteration));
                }

                // Get the external task status
                // Expected enum values from client: ACTIVE, INACTIVE, SUCCEEDED, FAILED
                String externalTaskStatus = gSrcClient.getGlobusTransferTaskStatus(externalTaskId);

                // If in a final state we are done. Use valueOf in case string is null
                boolean isTerminal;
                switch (String.valueOf(externalTaskStatus)) {
                    case "ACTIVE", "INACTIVE" -> isTerminal = false;
                    case "FAILED" -> {
                        isTerminal = true;
                        // Update status of taskChild.
                        if (taskChild.isOptional()) taskChild.setStatus(TransferTaskStatus.FAILED_OPT);
                        else taskChild.setStatus(TransferTaskStatus.FAILED);
                        taskChild.setEndTime(Instant.now());
                        taskChild = dao.updateTransferTaskChild(taskChild);
                    }
                    case "SUCCEEDED" -> {
                        isTerminal = true;
                        // Update status of taskChild.
                        taskChild.setStatus(TransferTaskStatus.COMPLETED);
                        taskChild.setEndTime(Instant.now());
                        taskChild = dao.updateTransferTaskChild(taskChild);
                    }
                    default -> {
                        msg = LibUtils.getMsg("FILES_TXFR_ASYNCH_BAD_STATUS", taskChild.getTenantId(), taskChild.getUsername(),
                                taskChild.getId(), taskChild.getTag(), taskChild.getUuid(), externalTaskId,
                                externalTaskStatus, iteration);
                        log.error(msg);
                        throw new ServiceException(msg);
                    }
                }
                if (isTerminal) break;
                iteration++;
            }
        } catch (DAOException ex) {
            msg = LibUtils.getMsg("FILES_TXFR_SVC_ERR1", taskChild.getTenantId(), taskChild.getUsername(),
                    "ChildStepTwoC", taskChild.getId(), taskChild.getTag(), taskChild.getUuid(), ex.getMessage());
            log.error(msg, ex);
            throw new ServiceException(msg, ex);
        }
        msg = LibUtils.getMsg("FILES_TXFR_CHILD_ASYNCH_END", taskChild.getTenantId(), taskChild.getUsername(),
                taskChild.getId(), taskChild.getTag(), taskChild.getUuid(), externalTaskId,
                srcUri.getSystemId(), srcRelPath, dstUri.getSystemId(), dstRelPath);
        log.trace(msg);
    }
}
