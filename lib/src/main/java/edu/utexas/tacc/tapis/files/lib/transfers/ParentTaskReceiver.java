package edu.utexas.tacc.tapis.files.lib.transfers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.ConnectionFactory;
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
import edu.utexas.tacc.tapis.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ParallelFlux;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.*;

import javax.inject.Inject;
import javax.inject.Named;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 *
 */

@Service
@Named
public class ParentTaskReceiver implements Runnable {

    private static final String PARENT_QUEUE = "tapis.files.transfers.parent";
    private static final String CHILD_QUEUE = "tapis.files.transfers.child";
    private static final Logger log = LoggerFactory.getLogger(ParentTaskReceiver.class);
    private static final int MAX_RETRIES = 5;
    private static final ObjectMapper mapper = TapisObjectMapper.getMapper();

    private final Receiver receiver;
    private final Sender sender;


    private final IRemoteDataClientFactory remoteDataClientFactory;
    private final SystemsClientFactory systemsClientFactory;
    private final TransfersService transfersService;

    @Inject
    public ParentTaskReceiver(@NotNull SystemsClientFactory systemsClientFactory,
                              @NotNull RemoteDataClientFactory remoteDataClientFactory,
                              @NotNull TransfersService transfersService) {
        ConnectionFactory connectionFactory = RabbitMQConnection.getInstance();
        ReceiverOptions receiverOptions = new ReceiverOptions()
            .connectionFactory(connectionFactory)
            .connectionSubscriptionScheduler(Schedulers.newElastic("parent-receiver"));
        SenderOptions senderOptions = new SenderOptions()
            .connectionFactory(connectionFactory)
            .resourceManagementScheduler(Schedulers.newElastic("parent-sender"));
        receiver = RabbitFlux.createReceiver(receiverOptions);
        sender = RabbitFlux.createSender(senderOptions);
        this.remoteDataClientFactory = remoteDataClientFactory;
        this.systemsClientFactory = systemsClientFactory;
        this.transfersService = transfersService;
    }

    private class MessagePair {

        private final AcknowledgableDelivery message;
        private final TransferTask task;

        public MessagePair(AcknowledgableDelivery message) throws IOException{
            this.message = message;
            this.task = deserializeTransferTaskMessage(message);
        }

        public AcknowledgableDelivery getMessage() {
            return message;
        }

        public TransferTask getTask() {
            return task;
        }
    }
    

    public void run() {
        streamMessages()
            .flatMap(message -> {
                try {
                    return  Mono.just(new MessagePair(message));
                } catch (IOException ex) {
                    return Mono.error(ex);
                }
            })
            .groupBy(this::groupByTenant)
            .map(group -> group.parallel().runOn(Schedulers.newParallel("pool", 8)))
            .subscribe(group -> this.processTenantGroup(group).subscribe());
    }

    private ParallelFlux<TransferTaskChild> processTenantGroup (ParallelFlux<MessagePair> tenantGroup) {
        return tenantGroup
            .flatMap(this::doRootListing)
            .flatMap(this::saveAndPublishChildTask);
    }

    private TransferTask deserializeTransferTaskMessage(AcknowledgableDelivery message) throws IOException {
        try {
            return mapper.readValue(message.getBody(), TransferTask.class);
        } catch (IOException ex) {
            log.error("invalid message", ex);
            throw ex;
        }
    }

    private Mono<String> groupByTenant(MessagePair pair) {
            return Mono.just(pair.getTask().getTenantId());
    }

    private Flux<AcknowledgableDelivery> streamMessages() {
        ConsumeOptions options = new ConsumeOptions();
        options.qos(1000);
        Flux<AcknowledgableDelivery> parentMessages = receiver.consumeManualAck(PARENT_QUEUE, options);
        return parentMessages.delaySubscription(sender.declareQueue(QueueSpecification.queue(PARENT_QUEUE)));
    }

    private Flux<TransferTaskChild> saveAndPublishChildTask(TransferTaskChild childTask) {
        //save task in db
        TransferTaskChild child;
        try {
            child = transfersService.createTransferTaskChild(childTask);
        } catch (ServiceException e) {
            return Flux.error(e);
        }

        try {
            byte[] messageBytes = mapper.writeValueAsBytes(child);
            sender.sendWithPublishConfirms(Flux.just(
                new OutboundMessage("", CHILD_QUEUE, messageBytes)
            )).subscribe();

        } catch (IOException e) {
            return Flux.error(e);
        }
        return Flux.just(child);
    }


    private Flux<TransferTaskChild> doRootListing(MessagePair pair) {
        TSystem sourceSystem;
        IRemoteDataClient sourceClient;
        TransferTask task = pair.getTask();
        AcknowledgableDelivery message = pair.getMessage();
        try {
            SystemsClient systemsClient = systemsClientFactory.getClient(task.getTenantId(), task.getUsername());
            sourceSystem = systemsClient.getSystemByName(task.getSourceSystemId(), null);
        } catch (TapisClientException | ServiceException ex) {
            return Flux.error(ex);
        }
        try {
            sourceClient = remoteDataClientFactory.getRemoteDataClient(sourceSystem, task.getUsername());
        } catch (IOException ex) {
            return Flux.error(ex);
        }

        List<FileInfo> fileListing;
        try {
            fileListing = sourceClient.ls(task.getSourcePath());
        } catch (IOException ex) {
            return Flux.error(ex);
        }

        Stream<TransferTaskChild> childTasks = fileListing.stream().map(f -> {
            return new TransferTaskChild(task, f.getPath());
        });
        message.ack();
        return Flux.fromStream(childTasks);
    }

}