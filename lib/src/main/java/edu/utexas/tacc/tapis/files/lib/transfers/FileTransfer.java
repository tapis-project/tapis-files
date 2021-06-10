package edu.utexas.tacc.tapis.files.lib.transfers;


import edu.utexas.tacc.tapis.files.lib.clients.HTTPClient;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import edu.utexas.tacc.tapis.files.lib.models.TransferURI;
import edu.utexas.tacc.tapis.files.lib.utils.Utils;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;

public class FileTransfer {
    
    private TransferTaskChild taskChild;
    private final FileTransfersDAO dao;
    private final TapisSystem sourceSystem;
    private final TapisSystem destSystem;
    private final RemoteDataClientFactory remoteDataClientFactory;

    private static final Logger log = LoggerFactory.getLogger(FileTransfer.class);
    
    public FileTransfer(@NotNull TransferTaskChild childTask,
                        @NotNull FileTransfersDAO dao,
                        @NotNull TapisSystem sourceSystem,
                        @NotNull TapisSystem destSystem,
                        @NotNull RemoteDataClientFactory remoteDataClientFactory) {
        this.taskChild = childTask;
        this.dao = dao;
        this.sourceSystem = sourceSystem;
        this.destSystem = destSystem;
        this.remoteDataClientFactory = remoteDataClientFactory;
    }

    private void run() throws ServiceException, IOException{
        String sourcePath;
        TransferURI destURL;
        TransferURI sourceURL = taskChild.getSourceURI();
        destURL = taskChild.getDestinationURI();

        IRemoteDataClient sourceClient = getSourceClient(sourceSystem);
        IRemoteDataClient destClient = getDestinationClient(destSystem);

        //Step 1: Update task in DB to IN_PROGRESS and increment the retries on this particular task
        try {
            taskChild.setStatus(TransferTaskStatus.IN_PROGRESS);
            taskChild.setRetries(taskChild.getRetries() + 1);
            taskChild = dao.updateTransferTaskChild(taskChild);
        } catch (DAOException ex) {
            throw new ServiceException(Utils.getMsg("FILES_TXFR_SVC_ERR1", taskChild.getTenantId(), taskChild.getUsername(),
                "chevronTwoA", taskChild.getId(), taskChild.getUuid(), ex.getMessage()), ex);
        }


        //Step 3: Stream the file contents to dest. While the InputStream is open,
        // we put a tap on it and send events that get grouped into 1 second intervals. Progress
        // on the child tasks are updated during the reading of the source input stream.
        try (InputStream sourceStream = sourceClient.getStream(sourcePath);
             ObservableInputStream observableInputStream = new ObservableInputStream(sourceStream)
        ) {
            // Observe the progress event stream, just get the last event from
            // the past 1 second.
            final TransferTaskChild finalTaskChild = taskChild;
            observableInputStream.getEventStream()
                .window(Duration.ofMillis(1000))
                .flatMap(window -> window.takeLast(1))
                .publishOn(Schedulers.boundedElastic())
                .flatMap((prog) -> this.updateProgress(prog, finalTaskChild))
                .subscribe();
            destClient.insert(destURL.getPath(), observableInputStream);

        }
    }

    private IRemoteDataClient getSourceClient(TapisSystem system) throws IOException, ServiceException {
        IRemoteDataClient sourceClient;
        if (taskChild.getSourceURI().toString().startsWith("https://") || taskChild.getSourceURI().toString().startsWith("http://")) {
            sourceClient = new HTTPClient(taskChild.getTenantId(), taskChild.getUsername(), sourceURL.toString(), destURL.toString());
            //This should be the full string URL such as http://google.com
        } else {
            if (sourceSystem.getEnabled() == null || !sourceSystem.getEnabled()) {
                String msg = Utils.getMsg("FILES_TXFR_SYS_NOTENABLED", taskChild.getTenantId(),
                    taskChild.getUsername(), taskChild.getId(), taskChild.getUuid(), sourceSystem.getId());
                throw new ServiceException(msg);
            }
            sourceClient = remoteDataClientFactory.getRemoteDataClient(taskChild.getTenantId(), taskChild.getUsername(),
                sourceSystem, taskChild.getUsername());

        }
        return sourceClient;
    }

    private IRemoteDataClient getDestinationClient(TapisSystem destSystem) throws ServiceException{
        //Step 2: Get clients for dest
        try {
            if (destSystem.getEnabled() == null || !destSystem.getEnabled()) {
                String msg = Utils.getMsg("FILES_TXFR_SYS_NOTENABLED", taskChild.getTenantId(),
                    taskChild.getUsername(), taskChild.getId(), taskChild.getUuid(), destSystem.getId());
                throw new ServiceException(msg);
            }
            IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(taskChild.getTenantId(), taskChild.getUsername(),
                destSystem, taskChild.getUsername());
            return destClient;
        } catch (IOException | ServiceException ex) {
            throw new ServiceException(Utils.getMsg("FILES_TXFR_SVC_ERR1", taskChild.getTenantId(), taskChild.getUsername(),
                "chevronTwoB", taskChild.getId(), taskChild.getUuid(), ex.getMessage()), ex);
        }
    }

    /**
     * @param bytesSent total bytes sent in latest update
     * @param taskChild The transfer task that is being worked on currently
     * @return Mono with number of bytes sent
     */
    private Mono<Long> updateProgress(Long bytesSent, TransferTaskChild taskChild) {
        taskChild.setBytesTransferred(bytesSent);
        try {
            dao.updateTransferTaskChild(taskChild);
            return Mono.just(bytesSent);
        } catch (DAOException ex) {
            log.error(Utils.getMsg("FILES_TXFR_SVC_ERR1", taskChild.getTenantId(), taskChild.getUsername(),
                "updateProgress", taskChild.getId(), taskChild.getUuid(), ex.getMessage()));
            return Mono.empty();
        }
    }
}
