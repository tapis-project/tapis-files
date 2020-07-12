package edu.utexas.tacc.tapis.files.lib.transfers;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.ConnectionFactory;
import com.sun.java.accessibility.util.Translator;
import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.json.TapisObjectMapper;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import edu.utexas.tacc.tapis.files.lib.rabbit.RabbitMQConnection;
import edu.utexas.tacc.tapis.files.lib.services.TransfersService;
import edu.utexas.tacc.tapis.files.lib.utils.SystemsClientFactory;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.statefulj.fsm.FSM;
import org.statefulj.fsm.TooBusyException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ParallelFlux;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.*;

import javax.inject.Inject;
import javax.inject.Named;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

/**
 *
 */

@Service
@Named
public class ParentTaskReceiver {


    private static final Logger log = LoggerFactory.getLogger(ParentTaskReceiver.class);
    private static final int MAX_RETRIES = 5;
    private static final ObjectMapper mapper = TapisObjectMapper.getMapper();
    private static final FSM<TransferTask> fsm = ParentTaskFSM.getFSM();


    private final IRemoteDataClientFactory remoteDataClientFactory;
    private final SystemsClientFactory systemsClientFactory;
    private final TransfersService transfersService;

    @Inject
    public ParentTaskReceiver(@NotNull SystemsClientFactory systemsClientFactory,
                              @NotNull RemoteDataClientFactory remoteDataClientFactory,
                              @NotNull TransfersService transfersService) {

        this.remoteDataClientFactory = remoteDataClientFactory;
        this.systemsClientFactory = systemsClientFactory;
        this.transfersService = transfersService;
    }

    public void run() {
        transfersService.streamParentMessages()
            .groupBy((message) -> {
                try {
                    return Mono.just(mapper.readValue(message.getBody(), TransferTask.class).getTenantId());
                } catch (IOException ex) {
                    log.error("invalid message", ex);
                    return Mono.error(ex);
                }
            }).map(Flux::parallel)
            .subscribe(group -> {
                group.runOn(Schedulers.newBoundedElastic(8, 1, "pool"))
                    .flatMap((taskMessage) -> {
                        try {
                            TransferTask task = mapper.readValue(taskMessage.getBody(), TransferTask.class);
                            fsm.onEvent(task, TransfersFSMEvents.TO_STAGING.name());
                            return Mono.just(task);
                        } catch (IOException ex) {
                            return Mono.error(ex);
                        } catch (TooBusyException ex) {
                            //This means the state transition to staging went awry
                            return Mono.error(ex);
                        }
                    })
                    .flatMap((task) -> {
                        try {
                            fsm.onEvent(task, TransfersFSMEvents.TO_STAGED.name());
                            return Mono.just(task);
                        } catch (TooBusyException ex) {
                            return Mono.error(ex);
                        }
                    }).subscribe();
            });
    }


//    public void run() {
//        streamMessages()
//            .flatMap(message -> {
//                try {
//                    return  Mono.just(new MessagePair(message));
//                } catch (IOException ex) {
//                    return Mono.error(ex);
//                }
//            })
//            .groupBy(this::groupByTenant)
//            .map(group -> group.parallel().runOn(Schedulers.newParallel("pool", 8)))
//            .subscribe(group -> this.processTenantGroup(group).subscribe());
//    }
//
//    private ParallelFlux<TransferTaskChild> processTenantGroup (ParallelFlux<MessagePair> tenantGroup) {
//        return tenantGroup
//            .flatMap(this::doRootListing)
//            .flatMap(this::saveAndPublishChildTask);
//    }
//
//    private TransferTask deserializeTransferTaskMessage(AcknowledgableDelivery message) throws IOException {
//        try {
//            return mapper.readValue(message.getBody(), TransferTask.class);
//        } catch (IOException ex) {
//            log.error("invalid message", ex);
//            throw ex;
//        }
//    }
//
//    private Mono<String> groupByTenant(MessagePair pair) {
//            return Mono.just(pair.getTask().getTenantId());
//    }
//
//


}