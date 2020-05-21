package edu.utexas.tacc.tapis.files.lib.transfers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.ConnectionFactory;
import edu.utexas.tacc.tapis.files.lib.cache.SSHConnectionCache;
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
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.hk2.utilities.ServiceLocatorUtilities;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.*;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 */

@Service @Named
public class ParentTaskReceiver {

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

    public ParentTaskReceiver(){
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
    }

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

    public void run() {
        ConsumeOptions options = new ConsumeOptions();
        options.qos(1000);
        Flux<AcknowledgableDelivery> messages = receiver.consumeManualAck(PARENT_QUEUE, options);
        messages.delaySubscription(sender.declareQueue(QueueSpecification.queue(PARENT_QUEUE)))
          .groupBy((message) -> {
              try {
                  TransferTask m = mapper.readValue(message.getBody(), TransferTask.class);
                  return m.getTenantId();
              } catch (IOException e) {
                  e.printStackTrace();
                  return Mono.empty();
              }
          })
          .subscribe(group -> {
              // This will be a flux of messages for a particular tenant
              group
                .flatMap(this::doRootListing)
                  .doOnError((throwable) -> {
                      //TODO: Mark transfer as FAILED
                      log.error("Could not complete task", throwable);
                  })
                .retryBackoff(MAX_RETRIES, Duration.ofSeconds(10), Duration.ofSeconds(5 * 60))
                .flatMap(this::saveAndPublishChildTask)
                .retryBackoff(MAX_RETRIES, Duration.ofSeconds(5), Duration.ofSeconds(60))
                .subscribe();
          });

    }

    private Flux<OutboundMessageResult> saveAndPublishChildTask(TransferTaskChild childTask) {
        //save task in db
        try {
            TransferTaskChild child = transfersService.createTransferTaskChild(childTask);
        } catch (ServiceException e) {
            return Flux.error(e);
        }

        try {
            byte[] messageBytes = mapper.writeValueAsBytes(childTask);
            return sender.sendWithPublishConfirms(Flux.just(
              new OutboundMessage("", CHILD_QUEUE, messageBytes)
            ));

        } catch (IOException e) {
            return Flux.error(e);
        }
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
            log.error("error getting system", ex);
            return Flux.error(ex);
        }
        try {
            sourceClient = remoteDataClientFactory.getRemoteDataClient(sourceSystem, task.getUsername());
        } catch (IOException ex) {
            String msg = String.format("Could not create client for system: %s", sourceSystem.getId());
            log.error(msg, ex);
            return Flux.error(ex);
        }

        List<FileInfo> fileListing;
        try {
            fileListing = sourceClient.ls(task.getSourcePath());
        } catch (IOException ex) {
            String msg = String.format("Could not perform listing on system: %s and path: %s", sourceSystem.getId(), task.getSourcePath());
            log.error(msg, ex);
            return Flux.error(ex);
        }

        Stream<TransferTaskChild> childTasks = fileListing.stream().map(f -> {
            return new TransferTaskChild(task, f.getPath());
        });
        log.info(childTasks.toString());
        message.ack();
        return Flux.fromStream(childTasks);
    }

    public static void main(String[] args) {
        ParentTaskReceiver receiver = new ParentTaskReceiver();
        receiver.run();
    }

}