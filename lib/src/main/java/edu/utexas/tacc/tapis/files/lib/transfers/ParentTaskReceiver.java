package edu.utexas.tacc.tapis.files.lib.transfers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.ConnectionFactory;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.*;

import java.io.IOException;

/**
 *
 */
public class ParentTaskReceiver {

    private static final String QUEUE = "tapis.files.transfers.parent";
    private static final Logger log = LoggerFactory.getLogger(ParentTaskReceiver.class);

    private final Receiver receiver;
    private final Sender sender;
    private static final ObjectMapper mapper = new ObjectMapper();

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

    public void consume(String queue) {

        ConsumeOptions options = new ConsumeOptions();
        options.qos(1000);
        Flux<AcknowledgableDelivery> messages = receiver.consumeManualAck(QUEUE, options);
        messages.delaySubscription(sender.declareQueue(QueueSpecification.queue(QUEUE)))
                .groupBy( message -> {

                    try {
                        TransferTask task = mapper.readValue(message.getBody(), TransferTask.class);
                        return task.getTenantId();
                    } catch (IOException e) {
                        return null;
                    }
                })
                .subscribe( group -> {
                    group.parallel()
                            .runOn(Schedulers.boundedElastic())
                            .subscribe( message -> {
                                message.ack();
                            });
                });
    }

    public void close() {
        this.sender.close();
        this.receiver.close();
    }

    public static void main(String[] args) throws Exception {
        ParentTaskReceiver receiver = new ParentTaskReceiver();
        receiver.consume(QUEUE);

    }

}