package edu.utexas.tacc.tapis.files.lib.services;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import edu.utexas.tacc.tapis.files.lib.utils.Utils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.AcknowledgableDelivery;
import reactor.rabbitmq.ConsumeOptions;
import reactor.rabbitmq.OutboundMessage;
import reactor.rabbitmq.OutboundMessageResult;
import reactor.util.retry.Retry;

import javax.inject.Inject;
import javax.ws.rs.ForbiddenException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Service
public class ParentTaskTransferService {

    private static final int MAX_RETRIES = 5;
    private final TransfersService transfersService;
    private final FileTransfersDAO dao;
    private static final ObjectMapper mapper = TapisObjectMapper.getMapper();
    private final RemoteDataClientFactory remoteDataClientFactory;
    private final FilePermsService permsService;
    private final SystemsCache systemsCache;
    private final IFileOpsService fileOpsService;
    private static final Logger log = LoggerFactory.getLogger(ParentTaskTransferService.class);

    @Inject
    public ParentTaskTransferService(TransfersService transfersService,
                                     FileTransfersDAO dao,
                                     IFileOpsService fileOpsService,
                                     FilePermsService permsService,
                                     RemoteDataClientFactory remoteDataClientFactory,
                                     SystemsCache systemsCache) {
        this.transfersService = transfersService;
        this.dao = dao;
        this.fileOpsService = fileOpsService;
        this.systemsCache = systemsCache;
        this.remoteDataClientFactory = remoteDataClientFactory;
        this.permsService = permsService;
    }

    private Mono<TransferTaskParent> deserializeParentMessage(AcknowledgableDelivery message) {
        try {
            TransferTaskParent parent = mapper.readValue(message.getBody(), TransferTaskParent.class);
            return Mono.just(parent);
        } catch (IOException ex) {
            // DO NOT requeue the message if it fails here!
            message.nack(false);
            return Mono.empty();
        }
    }

    private String groupByTenant(AcknowledgableDelivery message) throws ServiceException {
        try {
            return mapper.readValue(message.getBody(), TransferTaskParent.class).getTenantId();
        } catch (IOException ex) {
            message.nack(false);
            String msg = Utils.getMsg("FILES_TXFR_SVC_ERR11", ex.getMessage());
            log.error(msg);
            throw new ServiceException(msg, ex);
        }
    }



    public Flux<TransferTaskParent> runPipeline() {
        return transfersService.streamParentMessages()
            .groupBy(m -> {
                try {
                    return groupByTenant(m);
                } catch (ServiceException ex) {
                    return Mono.empty();
                }
            })
            .flatMap(group -> {
                Scheduler scheduler = Schedulers.newBoundedElastic(5, 10, "ParentPool:" + group.key());
                return group
                    .flatMap(m ->
                        deserializeParentMessage(m)
                            .flatMap(t1 -> Mono.fromCallable(() -> doParentChevronOne(t1))
                                .publishOn(scheduler)
                                .retryWhen(
                                    Retry.backoff(MAX_RETRIES, Duration.ofSeconds(1))
                                        .maxBackoff(Duration.ofMinutes(60))
                                        .filter(e -> e.getClass() == IOException.class)
                                )
                                .onErrorResume(e -> doErrorParentChevronOne(m, e, t1))
                            )
                            .flatMap(t2 -> {
                                m.ack();
                                return Mono.just(t2);
                            })
                    );
            });
    }


    /**
     * This method handles exceptions/errors if the parent task failed.
     *
     * @param m      message from rabbitmq
     * @param e      Throwable
     * @param parent TransferTaskParent
     * @return Mono TransferTaskParent
     */
    private Mono<TransferTaskParent> doErrorParentChevronOne(AcknowledgableDelivery m, Throwable e, TransferTaskParent parent) {
        log.error(Utils.getMsg("FILES_TXFR_SVC_ERR7", parent.toString()));
        log.error(Utils.getMsg("FILES_TXFR_SVC_ERR7", e));
        m.nack(false);

        //TODO: UPDATE this when the Optional stuff gets integrated
        try {
            TransferTask task = dao.getTransferTaskByID(parent.getTaskId());
            if (task == null) {
                return Mono.empty();
            }
            task.setStatus(TransferTaskStatus.FAILED);
            task.setEndTime(Instant.now());
            task.setErrorMessage(e.getMessage());
            dao.updateTransferTask(task);
        } catch (DAOException ex) {
            log.error(Utils.getMsg("FILES_TXFR_SVC_ERR8", parent.getTaskId(), parent.getUuid()));
        }

        parent.setStatus(TransferTaskStatus.FAILED);
        parent.setEndTime(Instant.now());
        parent.setErrorMessage(e.getMessage());
        try {
            parent = dao.updateTransferTaskParent(parent);
            // This should really never happen, it means that the parent with that ID
            // was not even in the database.
            if (parent == null) {
                return Mono.empty();
            }
            return Mono.just(parent);
        } catch (DAOException ex) {
            log.error(Utils.getMsg("FILES_TXFR_SVC_ERR9", parent.getTaskId(), parent.getUuid()));
        }
        return Mono.empty();
    }

    /**
     * This method checks the permissions on both the source and destination of the transfer.
     *
     * @param parentTask the TransferTaskParent
     * @return boolean is/is not permitted.
     * @throws ServiceException When api calls for permissions fail
     */
    private boolean checkPermissionsForParent(TransferTaskParent parentTask) throws ServiceException {
        // For http inputs no need to do any permission checking on the source
        boolean isHttpSource = parentTask.getSourceURI().getProtocol().equalsIgnoreCase("http");
        String tenantId = parentTask.getTenantId();
        String username = parentTask.getUsername();

        String srcSystemId = parentTask.getSourceURI().getSystemId();
        String srcPath = parentTask.getSourceURI().getPath();
        String destSystemId = parentTask.getDestinationURI().getSystemId();
        String destPath = parentTask.getDestinationURI().getPath();

        // If we have a tapis:// link, have to do the source perms check
        if (!isHttpSource) {
            boolean sourcePerms = permsService.isPermitted(tenantId, username, srcSystemId, srcPath, FileInfo.Permission.READ);
            if (!sourcePerms) {
                return false;
            }
        }
        boolean destPerms = permsService.isPermitted(tenantId, username, destSystemId, destPath, FileInfo.Permission.MODIFY);
        if (!destPerms) {
            return false;
        }
        return true;
    }


    /**
     * We prepare a "bill of materials" for the total transfer task. This includes doing a recursive listing and
     * inserting the records into the DB, then publishing all of the messages to rabbitmq. After that, the child task workers
     * will pick them up and begin the actual transferring of bytes.
     *
     * @param parentTask TransferTaskParent
     * @return Updated task
     * @throws ServiceException When a listing or DAO error occurs
     */
    private TransferTaskParent doParentChevronOne(TransferTaskParent parentTask) throws ServiceException, ForbiddenException {
        log.debug("***** DOING doParentChevronOne ****");
        log.debug(parentTask.toString());
        TapisSystem sourceSystem;
        IRemoteDataClient sourceClient;

        // Make sure its not already cancelled/failed
        if (parentTask.isTerminal()) return parentTask;

        boolean isPermitted = checkPermissionsForParent(parentTask);
        if (!isPermitted) {
            throw new ForbiddenException();
        }

        // Update the top level task first, if it is not already updated with the startTime
        try {
            TransferTask task = dao.getTransferTaskByID(parentTask.getTaskId());
            if (task.isTerminal()) return parentTask;
            if (task.getStartTime() == null) {
                task.setStartTime(Instant.now());
                task.setStatus(TransferTaskStatus.IN_PROGRESS);
                dao.updateTransferTask(task);
            }

            //update parent task status, start time
            parentTask.setStatus(TransferTaskStatus.IN_PROGRESS);
            parentTask.setStartTime(Instant.now());
            parentTask = dao.updateTransferTaskParent(parentTask);

        } catch (DAOException ex) {
            throw new ServiceException(Utils.getMsg("FILES_TXFR_SVC_ERR1", parentTask.getTenantId(), parentTask.getUsername(),
                "doParentChevronOneA", parentTask.getId(), parentTask.getUuid(), ex.getMessage()), ex);
        }

        try {
            TransferURI sourceURI = parentTask.getSourceURI();

            if (sourceURI.toString().startsWith("tapis://")) {
                sourceSystem = systemsCache.getSystem(parentTask.getTenantId(), sourceURI.getSystemId(), parentTask.getUsername());
                if (sourceSystem.getEnabled() == null || !sourceSystem.getEnabled()) {
                    String msg = Utils.getMsg("FILES_TXFR_SYS_NOTENABLED", parentTask.getTenantId(),
                        parentTask.getUsername(), parentTask.getId(), parentTask.getUuid(), sourceSystem.getId());
                    throw new ServiceException(msg);
                }
                sourceClient = remoteDataClientFactory.getRemoteDataClient(parentTask.getTenantId(), parentTask.getUsername(),
                    sourceSystem, parentTask.getUsername());

                //TODO: Retries will break this, should delete anything in the DB if it is a retry?
                List<FileInfo> fileListing;
                fileListing = fileOpsService.lsRecursive(sourceClient, sourceURI.getPath(), 10);
                List<TransferTaskChild> children = new ArrayList<>();
                long totalBytes = 0;
                for (FileInfo f : fileListing) {
                    // Only include the bytes from files. Posix folders are --usually-- 4bytes but not always, so
                    // it can make some weird totals that don't really make sense.
                    if (!f.isDir()) {
                        totalBytes += f.getSize();
                    }
                    TransferTaskChild child = new TransferTaskChild(parentTask, f);
                    children.add(child);
                }
                parentTask.setTotalBytes(totalBytes);
                parentTask.setStatus(TransferTaskStatus.STAGED);
                parentTask = dao.updateTransferTaskParent(parentTask);
                dao.bulkInsertChildTasks(children);
                children = dao.getAllChildren(parentTask);
                transfersService.publishBulkChildMessages(children);
            } else if (sourceURI.toString().startsWith("http://") || sourceURI.toString().startsWith("https://")) {
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
            return parentTask;
        } catch (DAOException | TapisException | IOException e) {
            throw new ServiceException(Utils.getMsg("FILES_TXFR_SVC_ERR1", parentTask.getTenantId(), parentTask.getUsername(),
                "doParentChevronOneB", parentTask.getId(), parentTask.getUuid(), e.getMessage()), e);
        }
    }

}
