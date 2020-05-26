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


/**
 *
 */
@Service @Named
public class ChildTaskReceiver implements Runnable {
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
    public ChildTaskReceiver(@NotNull SystemsClientFactory systemsClientFactory,
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

    public TransferTaskChild parseMessage(AcknowledgableDelivery message) throws IOException {
        TransferTaskChild task;
        task = mapper.readValue(message.getBody(), TransferTaskChild.class);
        log.info(task.toString());
        return task;
    }

    public void run() {

        ConsumeOptions options = new ConsumeOptions();
        options.qos(1000);
        Flux<AcknowledgableDelivery> messages = receiver.consumeManualAck(CHILD_QUEUE, options);
        messages.delaySubscription(sender.declareQueue(QueueSpecification.queue(CHILD_QUEUE)))
          .groupBy((message) -> {
              try {
                  TransferTaskChild m = mapper.readValue(message.getBody(), TransferTaskChild.class);
                  return m.getParentTaskId();
              } catch (IOException e) {
                  e.printStackTrace();
                  return Mono.empty();
              }
          })
          .map( group -> group.parallel().runOn(Schedulers.newParallel("pool", 10)))
          .subscribe(group -> {
              // This will be a flux of messages for a particular tenant
              group
                .flatMap( message -> this.fakeWork(message)
                  .retryBackoff(MAX_RETRIES, Duration.ofSeconds(10), Duration.ofSeconds(5 * 60))
                  .doOnError((throwable) -> {
                      //TODO: Mark transfer as FAILED
                      log.error("Could not complete task", throwable);
                  }))
                .subscribe();
          });


    }


    private Flux<TransferTaskChild> fakeWork(AcknowledgableDelivery message) {
        TransferTaskChild childTask;
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ex) {

        }

        try {
            childTask = parseMessage(message);
        } catch (IOException ex) {
            return Flux.error(ex);
        }


        message.ack();
        return Flux.just(childTask);
    }


}