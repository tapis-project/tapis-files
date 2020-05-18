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
import edu.utexas.tacc.tapis.files.lib.services.TransfersService;
import edu.utexas.tacc.tapis.files.lib.utils.SystemsClientFactory;
import edu.utexas.tacc.tapis.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.*;

import javax.inject.Inject;
import javax.inject.Named;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
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


    private IRemoteDataClientFactory remoteDataClientFactory;
    private SystemsClientFactory systemsClientFactory;
    private TransfersService transfersService;

    @Inject
    public ParentTaskReceiver(@NotNull SystemsClientFactory systemsClientFactory,
                              @NotNull RemoteDataClientFactory remoteDataClientFactory,
                              @NotNull TransfersService transfersService) {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUsername("dev");
        connectionFactory.setPassword("dev");
        connectionFactory.setVirtualHost("dev");
        connectionFactory.useNio();
        ReceiverOptions receiverOptions = new ReceiverOptions()
            .connectionFactory(connectionFactory)
            .connectionSubscriptionScheduler(Schedulers.newElastic("parent-receiver"));
        SenderOptions senderOptions = new SenderOptions()
            .connectionFactory(connectionFactory)
            .resourceManagementScheduler(Schedulers.newElastic("parent-sender"));
        this.receiver = RabbitFlux.createReceiver(receiverOptions);
        this.sender = RabbitFlux.createSender(senderOptions);
        this.remoteDataClientFactory = remoteDataClientFactory;
        this.systemsClientFactory = systemsClientFactory;
        this.transfersService = transfersService;
    }

    private Mono<TransferTask> deserializeTransferTaskMessage(AcknowledgableDelivery message) {
        try {
            return Mono.just(mapper.readValue(message.getBody(), TransferTask.class));
        } catch (IOException ex) {
            log.error("invalid message", ex);
            return Mono.empty();
        }
    }

    private Mono<String> groupByTenant( AcknowledgableDelivery message) {
        try {
            TransferTask task = mapper.readValue(message.getBody(), TransferTask.class);
            return Mono.just(task.getTenantId());
        } catch (IOException ex) {
            log.error("invalid message", ex);
            return Mono.empty();
        }
    }

    public void run() {
        ConsumeOptions options = new ConsumeOptions();
        options.qos(1000);
        Flux<AcknowledgableDelivery> parentMessages = receiver.consumeManualAck(PARENT_QUEUE, options);
        parentMessages.delaySubscription(sender.declareQueue(QueueSpecification.queue(PARENT_QUEUE)))
            .groupBy((message) -> {
                try {
                    TransferTask m = mapper.readValue(message.getBody(), TransferTask.class);
                    return m.getTenantId();
                } catch (IOException e) {
                    return Mono.empty();
                }
            })
            .map(group -> group.parallel().runOn(Schedulers.newParallel("pool")))
            .subscribe(group -> {
                // This will be a flux of messages for a particular tenant
                group
                    .flatMap(message ->
                        this.fakeWork(message)
                            .retryBackoff(MAX_RETRIES, Duration.ofSeconds(10), Duration.ofSeconds(300))
                            .doOnError( throwable -> {
                                //TODO: Mark transfer as FAILED
                                message.nack(true);
                                log.error("Could not complete task", throwable);
                            })
                    )
                    .doOnError(throwable -> log.error("what", throwable))
                    .flatMap(this::saveAndPublishChildTask)
                    .doOnError(throwable -> log.error("wtf2", throwable))
                    .subscribe();
            });

    }


    private Flux<TransferTaskChild> fakeWork(AcknowledgableDelivery message) {

        Mono.fromCallable(() -> {
            try {
                Thread.sleep(1000);
                log.info("SLEPT");
                return true;
            } catch (InterruptedException ex) {
                return false;
            }
        }).subscribeOn(Schedulers.boundedElastic()).subscribe();

        TransferTask task;

        try {
            task = mapper.readValue(message.getBody(), TransferTask.class);
            log.info(task.toString());
        } catch (IOException ex) {
            return Flux.error(ex);
        }

        TransferTask newTask;
        try {
            newTask = transfersService.createTransfer(
                task.getUsername(),
                task.getTenantId(),
                task.getSourceSystemId(),
                task.getSourcePath(),
                task.getDestinationSystemId(),
                task.getDestinationPath()
            );
        } catch (ServiceException ex) {
            return Flux.error(ex);
        }
        message.ack();
        TransferTaskChild taskChild = new TransferTaskChild(newTask, "/a/b/c");

        return Flux.just(taskChild);
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


    private Flux<TransferTaskChild> doRootListing(AcknowledgableDelivery message) {
        TSystem sourceSystem;
        IRemoteDataClient sourceClient;
        TransferTaskChild task;

        try {
            task = mapper.readValue(message.getBody(), TransferTaskChild.class);
            log.info(task.toString());
        } catch (IOException ex) {
            return Flux.error(ex);
        }

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