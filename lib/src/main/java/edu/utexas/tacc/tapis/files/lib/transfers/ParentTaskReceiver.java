package edu.utexas.tacc.tapis.files.lib.transfers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.ConnectionFactory;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.utils.SystemsClientFactory;
import edu.utexas.tacc.tapis.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.sharedapi.security.TenantManager;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import org.apache.commons.io.input.ObservableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.*;

import javax.crypto.spec.PSource;
import javax.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.Observable;

/**
 *
 */
public class ParentTaskReceiver implements Runnable {

    private static final String QUEUE = "tapis.files.transfers.parent";
    private static final Logger log = LoggerFactory.getLogger(ParentTaskReceiver.class);

    private final Receiver receiver;
    private final Sender sender;
    private static final ObjectMapper mapper = new ObjectMapper();

    @Inject
    IRemoteDataClientFactory remoteDataClientFactory;

   @Inject
   SystemsClientFactory systemsClientFactory;

    public ParentTaskReceiver() {
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

    private TransferTask deserializeTransferTaskMessage(AcknowledgableDelivery message)  throws IOException {
        return mapper.readValue(message.getBody(), TransferTask.class);
    }

    @Override
    public void run() {

        ConsumeOptions options = new ConsumeOptions();
        options.qos(1000);
        Flux<AcknowledgableDelivery> messages = receiver.consumeManualAck(QUEUE, options);
        messages.delaySubscription(sender.declareQueue(QueueSpecification.queue(QUEUE)))
          .flatMap(message -> {
              try {
                  return Flux.just(deserializeTransferTaskMessage(message));
              } catch (IOException ex) {
                  return Flux.empty();
              }

          })
          .groupBy(TransferTask::getTenantId)
          .map(g -> {
              return g.parallel().runOn(Schedulers.parallel());
          })
          .subscribe(group -> {
              group
                .map(this::doRootListing)
                .subscribe();
          });
    }

    private Flux<FileInfo> doRootListing(TransferTask task) throws ServiceException {
        TSystem sourceSystem;
        IRemoteDataClient sourceClient;

        try {
            SystemsClient systemsClient = systemsClientFactory.getClient(task.getTenantId(), task.getUsername());
            sourceSystem = systemsClient.getSystemByName(task.getSourceSystemId(), null);
        } catch (TapisClientException | ServiceException ex) {
            throw new ServiceException("Could not retrieve system {}");
        }

        try {
            sourceClient = remoteDataClientFactory.getRemoteDataClient(sourceSystem, task.getUsername());
        } catch (IOException ex) {
            String msg = String.format("Could not create client for system: {}", sourceSystem.getId());
            throw new ServiceException(msg);
        }

        List<FileInfo> fileListing;
        try {
            fileListing = sourceClient.ls(task.getSourcePath());
        } catch (IOException ex) {
            String msg = String.format("Could not perform listing on system: {} and path: {}", sourceSystem.getId(), task.getSourcePath());
            throw new ServiceException(msg);
        }


    }


    public void close() {
        this.sender.close();
        this.receiver.close();
    }

    public static void main(String[] args) throws Exception {
        ParentTaskReceiver receiver = new ParentTaskReceiver();
        Thread thread = new Thread(receiver);
        thread.start();
    }

}